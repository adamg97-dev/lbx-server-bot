import discord
from discord.ext import commands, tasks
import sqlite3
import asyncio
import requests
from bs4 import BeautifulSoup
from letterboxdpy.search import Search
from letterboxdpy.movie import Movie
from letterboxdpy.user import User
import statistics
import time
import os   # ← ADD THIS LINE AT THE VERY TOP (after the other imports)

# ================== CONFIG ==================
TOKEN = os.getenv("TOKEN")   # ← Railway will supply this automatically
# Do NOT put your real token here anymore
PREFIX = "."
DB_FILE = "lbx_server.db"
# ===========================================

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ============== DATABASE SETUP ==============
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS users 
             (discord_id INTEGER PRIMARY KEY, lbx_username TEXT UNIQUE)''')
c.execute('''CREATE TABLE IF NOT EXISTS films 
             (slug TEXT PRIMARY KEY, title TEXT, year INTEGER)''')
c.execute('''CREATE TABLE IF NOT EXISTS ratings 
             (discord_id INTEGER, film_slug TEXT, rating REAL, 
              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (discord_id, film_slug))''')
conn.commit()

# ============== NEW: FULL RATINGS SCRAPER (every page) ==============
def scrape_all_ratings(lbx_username: str):
    ratings = {}
    page = 1
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    while True:
        url = f"https://letterboxd.com/{lbx_username.lower()}/ratings/page/{page}/"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            posters = soup.find_all("li", class_="poster-container")
            if not posters:
                break

            for poster in posters:
                # Get slug
                link = poster.find("a", href=lambda h: h and "/film/" in h)
                if not link:
                    continue
                slug = link["href"].strip("/").split("/")[-1]

                # Get rating - multiple robust fallbacks
                rating = None
                for selector in [
                    ("span", "display-rating"),
                    ("span", "rating"),
                    ("div", {"data-rating": True})
                ]:
                    tag = poster.find(*selector) if isinstance(selector[1], str) else poster.find(selector[0], selector[1])
                    if tag:
                        if isinstance(tag, dict) and "data-rating" in tag:  # data-rating case
                            rating = int(tag["data-rating"]) / 2.0
                            break
                        text = tag.get_text(strip=True)
                        if text:
                            try:
                                rating = float(text.replace("½", ".5"))
                                break
                            except:
                                pass

                # Star fallback
                if rating is None:
                    star_text = poster.find(string=lambda t: t and any(c in str(t) for c in ["★", "½"]))
                    if star_text:
                        s = str(star_text).strip()
                        rating = s.count("★") + (0.5 * s.count("½"))

                if rating and 0.5 <= rating <= 5.0:
                    ratings[slug] = rating

            print(f"✅ Page {page} → {len(posters)} films for {lbx_username}")
            page += 1
            time.sleep(1.5)  # be nice to Letterboxd
            if page > 200:
                break
        except Exception as e:
            print(f"Page {page} failed for {lbx_username}: {e}")
            break
    return ratings

# ============== FULL CACHE FOR ONE USER ==============
def full_cache_user(discord_id: int, lbx_username: str):
    ratings_dict = scrape_all_ratings(lbx_username)
    for slug, rating in ratings_dict.items():
        c.execute("""INSERT OR REPLACE INTO ratings 
                     (discord_id, film_slug, rating) VALUES (?, ?, ?)""",
                  (discord_id, slug, rating))
    conn.commit()
    return len(ratings_dict)

# ============== AUTO CACHE EVERY 24 HOURS ==============
@tasks.loop(hours=24)
async def auto_full_cache():
    print("🔄 Starting daily full ratings cache for all members...")
    c.execute("SELECT discord_id, lbx_username FROM users")
    users = c.fetchall()
    for d_id, username in users:
        try:
            num = full_cache_user(d_id, username)
            print(f"   Cached {num} ratings for {username}")
            await asyncio.sleep(30)  # safe spacing between users
        except Exception as e:
            print(f"   Error caching {username}: {e}")
    print("✅ Daily full cache complete!")

# ============== HELPERS (updated) ==============
def get_film_info(movie_title: str):
    # (unchanged from your current code)
    try:
        search = Search(movie_title, 'films')
        results = search.get_results(max=1)
        if not results:
            return None
        slug = results[0]['slug']
        film = Movie(slug)
        title = getattr(film, 'name', movie_title)
        year = getattr(film, 'year', None)
        c.execute("INSERT OR REPLACE INTO films VALUES (?, ?, ?)", (slug, title, year))
        conn.commit()
        return {'slug': slug, 'title': title, 'year': year}
    except:
        return None

def get_user_rating(lbx_username: str, film_slug: str):
    # (your existing per-film scraper - kept as fallback)
    url = f"https://letterboxd.com/{lbx_username.lower()}/film/{film_slug}/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        tag = soup.find("span", class_="display-rating")
        if tag and tag.text.strip():
            try: return float(tag.text.strip())
            except: pass
        
        tag = soup.find("div", {"data-rating": True})
        if tag: return int(tag["data-rating"]) / 2.0
        
        star_tag = soup.find(string=lambda t: t and ('★' in t or '½' in t))
        if star_tag:
            stars = star_tag.count('★') + (star_tag.count('½') * 0.5)
            if 0.5 <= stars <= 5.0: return stars
    except:
        pass
    return None

# ============== HYBRID SERVER RATINGS (DB first + fallback) ==============
async def get_server_ratings(slug: str, ctx):
    c.execute("SELECT discord_id, lbx_username FROM users")
    all_members = c.fetchall()
    
    ratings = []
    missing = []
    
    # Check cache first
    for d_id, username in all_members:
        c.execute("SELECT rating FROM ratings WHERE discord_id=? AND film_slug=?", (d_id, slug))
        row = c.fetchone()
        if row and row[0] is not None:
            member = ctx.guild.get_member(d_id)
            name = member.display_name if member else username
            ratings.append((name, row[0]))
        else:
            missing.append((d_id, username))
    
    # Fallback scrape only the missing ones
    for d_id, username in missing:
        rating = get_user_rating(username, slug)
        if rating is not None:
            c.execute("INSERT OR REPLACE INTO ratings (discord_id, film_slug, rating) VALUES (?, ?, ?)",
                      (d_id, slug, rating))
            conn.commit()
            member = ctx.guild.get_member(d_id)
            name = member.display_name if member else username
            ratings.append((name, rating))
        await asyncio.sleep(0.4)
    
    return ratings

# ============== ALL YOUR COMMANDS (now use the hybrid cache) ==============
# (connect, connected, avg, who, compare, moviebattles, polarizing, goat, top, worst, compatible)
# → I kept them exactly the same as your current code, just swapped get_server_ratings to the new hybrid version above.
# Paste your existing command blocks here (they will work unchanged because get_server_ratings is now smarter).

# ... [paste all your @bot.command blocks from connect through compatible here - they are identical to what you already have]

# ============== NEW: MANUAL FULL CACHE ==============
@bot.command(name="cacheall")
async def cacheall(ctx):
    await ctx.send("🚀 Starting **full ratings cache** for every member… (5–30 min)")
    c.execute("SELECT discord_id, lbx_username FROM users")
    users = c.fetchall()
    total = 0
    for i, (d_id, username) in enumerate(users, 1):
        try:
            num = full_cache_user(d_id, username)
            total += num
            await ctx.send(f"✅ **{i}/{len(users)}** {username} → {num} ratings cached")
            await asyncio.sleep(5)
        except Exception as e:
            await ctx.send(f"❌ Error on {username}: {e}")
    await ctx.send(f"🎉 **Full cache complete!** Total ratings in DB: **{total}**")

# ============== READY ==============
@bot.event
async def on_ready():
    print(f"✅ {bot.user} is online — FULL CACHE MODE ENABLED!")
    auto_full_cache.start()   # starts the 24h auto cache
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="your ratings"))

bot.run(TOKEN)
