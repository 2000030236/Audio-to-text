# Endpoint to check transcription access

# Place this after app = FastAPI()
from fastapi import Depends, Cookie
import asyncio
from typing import Optional
from sum import summarize_text, generate_topic_wise_consolidation, generate_unique_text
import yt_dlp


from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import uuid
import traceback
import stripe
import sqlite3
import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from fastapi.middleware.cors import CORSMiddleware
import threading
from pydantic import BaseModel
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import logging

# Setup logging
logging.basicConfig(
    filename='app_debug.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)

MAIN_WHISPER_URL = "ws://97.99.18.159:9002/"
BACKUP_WHISPER_URL = "ws://97.99.18.159:9003/"


DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            name TEXT,
            login_time TEXT,
            subscribed INTEGER DEFAULT 0,
            stripe_subscription_id TEXT,
            free_trial_used INTEGER DEFAULT 0,
            free_trial_count INTEGER DEFAULT 0
        )
    """)
    # New transcription queue table
    c.execute("""
        CREATE TABLE IF NOT EXISTS transcription_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            transcription_text TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            summary TEXT,
            title TEXT
        )
    """)
    
    # NEW: Dedicated table for batch transcriptions
    c.execute("""
        CREATE TABLE IF NOT EXISTS batch_transcriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            transcription_text TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME,
            summary TEXT,
            title TEXT,
            batch_id TEXT
        )
    """)

    # Ensure batch_id exists
    try:
        c.execute("ALTER TABLE batch_transcriptions ADD COLUMN batch_id TEXT")
    except: pass
    
    # Ensure columns exist if table was already there
    try:
        c.execute("ALTER TABLE transcription_queue ADD COLUMN summary TEXT")
    except: pass
    try:
        c.execute("ALTER TABLE transcription_queue ADD COLUMN title TEXT")
    except: pass

    # NEW: Group Tables
    c.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS group_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT DEFAULT 'member', -- 'admin' or 'member'
            joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(group_id) REFERENCES groups(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    # NEW: Table for storing consolidated bulk results
    c.execute("""
        CREATE TABLE IF NOT EXISTS bulk_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            batch_id TEXT,
            topic_wise_result TEXT,
            urls TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # NEW: Table for logging shared results
    c.execute("""
        CREATE TABLE IF NOT EXISTS shared_transcriptions_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER NOT NULL,
            recipient_emails TEXT,
            content TEXT,
            title TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
init_db()






def send_email(to_email, subject, body, from_email, app_password, smtp_server='smtp.gmail.com', smtp_port=587, is_html=False, attachment=None):
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'html' if is_html else 'plain'))
    
    if attachment:
        try:
            filename, content, ctype = attachment
            from email.mime.base import MIMEBase
            from email import encoders
            
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(content)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
            msg.attach(part)
        except Exception as e:
            logging.error(f"Failed to attach file: {e}")
    
    logging.info(f"Debug: Attempting to send email to {to_email}")
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(from_email, app_password)
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()
        logging.info(f"Debug: Email sent to {to_email}")
        return True
    except Exception as e:
        logging.error(f"Debug: Email failed: {e}")
        return False

def save_user(email, name):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    # Check if user exists
    c.execute("SELECT id FROM users WHERE email=?", (email,))
    row = c.fetchone()
    if row:
        # Update name and login_time for existing user
        c.execute("UPDATE users SET name=?, login_time=? WHERE email=?", (name, datetime.datetime.now().isoformat(), email))
    else:
        # Insert new user with all required fields
        c.execute("INSERT INTO users (email, name, login_time, free_trial_used) VALUES (?, ?, ?, 0)",
                  (email, name, datetime.datetime.now().isoformat()))
    conn.commit()
    conn.close()

def set_user_subscribed(email, stripe_subscription_id=None):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    if stripe_subscription_id:
        c.execute("UPDATE users SET subscribed=1, stripe_subscription_id=? WHERE email=?", (stripe_subscription_id, email))
    else:
        c.execute("UPDATE users SET subscribed=1 WHERE email=?", (email,))
    conn.commit()
    conn.close()

def get_user_subscription(email):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT subscribed, stripe_subscription_id, free_trial_used, free_trial_count FROM users WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if not row:
        return (False, None, False, 0)
    
    subscribed = bool(row[0])
    stripe_id = row[1]
    trial_used = bool(row[2])
    trial_count = row[3] if row[3] is not None else 0
    
    return (subscribed, stripe_id, trial_used, trial_count)
# --------------------------------------------------------------
# Helper – fetch a user row (id, name, email) from the DB
# --------------------------------------------------------------
def get_user_by_email(email: str):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    cur = conn.cursor()
    cur.execute("SELECT id, name, email FROM users WHERE email=?", (email,))
    row = cur.fetchone()
    conn.close()
    return row   # (id, name, email) or None


from fastapi import Form, UploadFile, File
import uuid, shutil
app = FastAPI()
# --- Group API Models ---
class CreateGroupRequest(BaseModel):
    group_name: str
    member_emails: list[str] = []

@app.post("/api/groups/create")
def create_group(request: CreateGroupRequest, email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"error": "User not found"}, status_code=404)
    
    creator_id = user_row[0]
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    
    try:
        # 1. Create Group
        c.execute("INSERT INTO groups (name, created_by) VALUES (?, ?)", (request.group_name, creator_id))
        group_id = c.lastrowid
        
        # 2. Add Creator as Admin
        c.execute("INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'admin')", (group_id, creator_id))
        
        # 3. Add Members
        added_count = 0
        not_found_emails = []
        
        for member_email in request.member_emails:
            member_email = member_email.strip()
            if not member_email or member_email == email: continue # Skip self or empty
            
            # Find user ID or Create placeholder
            c.execute("SELECT id FROM users WHERE email=?", (member_email,))
            row = c.fetchone()
            
            if row:
                mid = row[0]
            else:
                # Auto-create user placeholder
                c.execute("INSERT INTO users (email, name, free_trial_used) VALUES (?, ?, 0)", 
                          (member_email, member_email.split('@')[0]))
                mid = c.lastrowid
                
            # Check if already in group
            c.execute("SELECT id FROM group_members WHERE group_id=? AND user_id=?", (group_id, mid))
            if not c.fetchone():
                c.execute("INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'member')", (group_id, mid))
                added_count += 1
        
        conn.commit()
        return {
            "status": "success", 
            "group_id": group_id, 
            "message": f"Group '{request.group_name}' created.",
            "added_count": added_count,
            "not_found_emails": not_found_emails
        }
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Group creation error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

@app.get("/api/groups/list")
def list_user_groups(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"groups": []})
    
    user_id = user_row[0]
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    try:
        # Get groups where user is a member/admin
        # We join groups and group_members to get the group name and role
        c.execute("""
            SELECT g.id, g.name, gm.role, g.created_at, 
                   (SELECT COUNT(*) FROM group_members WHERE group_id = g.id) as member_count
            FROM groups g
            JOIN group_members gm ON g.id = gm.group_id
            WHERE gm.user_id = ?
            ORDER BY g.created_at DESC
        """, (user_id,))
        rows = c.fetchall()
        
        groups = []
        for r in rows:
            groups.append({
                "id": r[0],
                "name": r[1],
                "role": r[2],
                "created_at": r[3],
                "member_count": r[4]
            })
            
        return {"groups": groups}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

@app.get("/api/groups/{group_id}/members")
def get_group_members(group_id: int, email: str = Cookie(None)):
    if not email: return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    try:
        # Verify access (must be member of the group)
        user = get_user_by_email(email)
        if not user: return JSONResponse({"error": "User not found"}, status_code=404)
        user_id = user[0]
        
        c.execute("SELECT 1 FROM group_members WHERE group_id=? AND user_id=?", (group_id, user_id))
        if not c.fetchone():
            return JSONResponse({"error": "Access denied"}, status_code=403)
            
        c.execute("""
            SELECT u.email, u.name, gm.role, gm.joined_at 
            FROM group_members gm
            JOIN users u ON gm.user_id = u.id
            WHERE gm.group_id = ?
            ORDER BY gm.role, u.name
        """, (group_id,))
        
        members = []
        for r in c.fetchall():
            members.append({"email": r[0], "name": r[1], "role": r[2], "joined_at": r[3]})
            
        return {"members": members}
    finally:
        conn.close()

class AddMemberRequest(BaseModel):
    group_id: int
    email: str

@app.post("/api/groups/add-member")
def add_group_member_endpoint(req: AddMemberRequest, email: str = Cookie(None)):
    if not email: return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    try:
        # Check if requester is admin of group
        # (For simplicity allowing any member to add for now, or strict admin? User said "we click on group... add members", implies they can do it. Let's restrict to group admins or just allow for now.)
        # Let's enforce that the requester must be in the group at least.
        requester = get_user_by_email(email)
        if not requester: return JSONResponse({"error": "User not found"}, status_code=404)
        
        c.execute("SELECT role FROM group_members WHERE group_id=? AND user_id=?", (req.group_id, requester[0]))
        row = c.fetchone()
        if not row:
             return JSONResponse({"error": "Access denied"}, status_code=403)
        
        # Check target user
        target_email = req.email.strip()
        target = get_user_by_email(target_email)
        
        if target:
            target_id = target[0]
        else:
            # Auto-create user placeholder if they don't exist
            c.execute("INSERT INTO users (email, name, free_trial_used) VALUES (?, ?, 0)", 
                      (target_email, target_email.split('@')[0]))
            target_id = c.lastrowid
        
        # Check validity
        c.execute("SELECT 1 FROM group_members WHERE group_id=? AND user_id=?", (req.group_id, target_id))
        if c.fetchone():
             return JSONResponse({"message": "User already in group"}, status_code=400)
             
        c.execute("INSERT INTO group_members (group_id, user_id, role) VALUES (?, ?, 'member')", (req.group_id, target_id))
        conn.commit()
        return {"status": "success", "message": "Member added"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

@app.post("/api/groups/delete")
def delete_group(req: dict, email: str = Cookie(None)):
    if not email: return JSONResponse({"error": "Not logged in"}, status_code=401)
    group_id = req.get("group_id")
    if not group_id: return JSONResponse({"error": "Missing group_id"}, status_code=400)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        # Verify requester is admin of the group
        user = get_user_by_email(email)
        if not user: return JSONResponse({"error": "User not found"}, status_code=404)
        user_id = user[0]
        
        c.execute("SELECT role FROM group_members WHERE group_id=? AND user_id=? AND role='admin'", (group_id, user_id))
        if not c.fetchone():
            return JSONResponse({"error": "Only group admins can delete the group"}, status_code=403)
        
        # Delete entries manually for safety
        c.execute("DELETE FROM group_members WHERE group_id=?", (group_id,))
        c.execute("DELETE FROM groups WHERE id=?", (group_id,))
        conn.commit()
        return {"status": "success", "message": "Group deleted successfully"}
    except Exception as e:
        conn.rollback()
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

class RemoveMemberRequest(BaseModel):
    group_id: int
    email: str

@app.post("/api/groups/remove-member")
def remove_group_member_endpoint(req: RemoveMemberRequest, email: str = Cookie(None)):
    if not email: return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    try:
        # Check if requester is admin of group
        requester = get_user_by_email(email)
        if not requester: return JSONResponse({"error": "User not found"}, status_code=404)
        requester_id = requester[0]

        c.execute("SELECT role FROM group_members WHERE group_id=? AND user_id=? AND role='admin'", (req.group_id, requester_id))
        if not c.fetchone():
             return JSONResponse({"error": "Only admins can remove members"}, status_code=403)
        
        # Check target user
        target = get_user_by_email(req.email)
        if not target:
             return JSONResponse({"error": "Member not found"}, status_code=404)
        target_id = target[0]

        # Don't allow removing themselves
        if target_id == requester_id:
             return JSONResponse({"error": "Admins cannot remove themselves."}, status_code=400)

        c.execute("DELETE FROM group_members WHERE group_id=? AND user_id=?", (req.group_id, target_id))
        conn.commit()
        return {"status": "success", "message": "Member removed"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

class ShareRequest(BaseModel):
    emails: list[str]
    content: str
    title: str = "Shared Information"

@app.post("/api/groups/share")
def share_transcription(req: ShareRequest, email: str = Cookie(None)):
    if not email: return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row: return JSONResponse({"error": "User not found"}, status_code=404)
    sender_id = user_row[0]

    # Log to DB
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO shared_transcriptions_log (sender_id, recipient_emails, content, title)
            VALUES (?, ?, ?, ?)
        """, (sender_id, ",".join(req.emails), req.content, req.title))
        conn.commit()
    except Exception as e:
        logging.error(f"Error logging share: {e}")
        conn.rollback()
    finally:
        conn.close()

    # Send emails in background
    def _send_shares():
        # Convert newlines to <br> for robust email rendering, as email clients often ignore white-space: pre-wrap
        formatted_content = req.content.replace("\n", "<br>")
        for recipient in req.emails:
            send_email(
                to_email=recipient,
                subject=f"Shared Transcription: {req.title}",
                body=f"<html><body><h3>Shared Content</h3><p>A transcription has been shared with you:</p><div style='background:#f3f4f6;padding:15px;border-radius:8px;font-family:Arial,sans-serif;'>{formatted_content}</div></body></html>",
                from_email="saran.d@mlopssol.com",
                app_password="hsdy vntq vieu gjzu",
                is_html=True
            )
            
    threading.Thread(target=_send_shares).start()
    return {"status": "success", "message": f"Sending to {len(req.emails)} recipients"}

@app.on_event("startup")
async def startup_event():
    # Clear any processing jobs that might have been left over from a previous run/crash
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        c = conn.cursor()
        c.execute("UPDATE transcription_queue SET status='failed', transcription_text='System restarted' WHERE status='processing'")
        if c.rowcount > 0:
            logging.info(f"Startup: Cleared {c.rowcount} stuck processing jobs.")
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Startup cleanup error: {e}")

# ============= BUSINESS GRADED QUEUE SYSTEM =============
import asyncio
import websockets
import json
import threading

# Configuration
MAX_CONCURRENT_REQUESTS = 1

# Stats (updated by worker)
queue_stats = {
    "current_processing": 0,
    "current_waiting": 0,
    "total_processed": 0
}

def get_user_processing_count(user_id):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM transcription_queue WHERE user_id=? AND status='processing'", (user_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

def get_user_by_id(user_id):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT id, name, email FROM users WHERE id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row
    
def clean_youtube_url(url):
    """
    Removes playlist parameters ('list', 'index') from YouTube URLs
    to ensure we only process the single video.
    """
    try:
        if not url: return url
        parsed = urlparse(url)
        if "youtube.com" in parsed.netloc or "youtu.be" in parsed.netloc:
            query = parse_qs(parsed.query)
            # Remove playlist-related params
            query.pop("list", None)
            query.pop("index", None)
            query.pop("start_radio", None)
            
            # Reconstruct URL
            new_query = urlencode(query, doseq=True)
            new_parsed = parsed._replace(query=new_query)
            return urlunparse(new_parsed)
    except:
        pass
    return url

def resolve_youtube_url_locally(video_url):
    """
    Resolves a YouTube URL to a direct media stream URL locally.
    This bypasses the remote server's IP block/bot detection by doing the extraction here.
    """
    import yt_dlp
    try:
        logging.info(f"Attempting to resolve direct URL for: {video_url}")
        ydl_opts = {
            'format': 'bestaudio/best',
            'quiet': True,
            'noplaylist': True,
            'force_ipv4': True,
            'no_color': True,
            'extract_flat': False,
            'cookiefile': 'cookies.txt' if os.path.exists('cookies.txt') else None,
            'sleep_interval': 1,
        }
        try:
            logging.info(f"Using yt-dlp to extract info for: {video_url}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                if info and 'url' in info:
                    direct_url = info['url']
                    logging.info(f"Successfully resolved to direct URL (len={len(direct_url)})")
                    return direct_url
                else:
                    logging.warning(f"yt-dlp returned info but no 'url' found for {video_url}")
        except Exception as e:
            logging.error(f"yt-dlp extraction error: {e}")
            raise e
    except Exception as e:
        logging.error(f"Local URL resolution failed for {video_url}: {e}")
    
    return video_url # Fallback to original if we fail

async def process_video_task(transcription_id, video_url, video_title, user_id, user_email, table_name="transcription_queue"):
    """
    Actual processing logic executed by the worker.
    """
    logging.info(f"Starting processing for job {transcription_id}: {video_title} (Table: {table_name})")
    
    # Double check trial limits before starting expensive resource
    if user_email:
        subscribed, _, _, free_trial_count = get_user_subscription(user_email)
        if not subscribed and free_trial_count >= 10:
             update_transcription_entry(transcription_id, "failed", "Your free trial is completed. For transcribing more, please subscribe.", table_name=table_name)
             return

    # Status is already set to 'processing' by the worker/lock
    
    # RESOLVE URL LOCALLY - Removed as it causes direct URL downloads (videoplayback) which are slow and unnamed.
    # Send the original clean YouTube URL so the remote server can use its own optimized handling.
    final_url = clean_youtube_url(video_url)
    
    max_retries = 3
    retry_delay = 5
    
    async def heartbeat():
        while True:
            await asyncio.sleep(60)
            try:
                conn = sqlite3.connect(DB_PATH, timeout=20)
                c = conn.cursor()
                c.execute(f"UPDATE {table_name} SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (transcription_id,))
                conn.commit()
                conn.close()
            except: pass

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        success = False
        response_received = False
        for attempt in range(max_retries):
            if success: break
            try:
                # Whisper Fallback logic for this attempt
                connection_attempts = [
                    (MAIN_WHISPER_URL, 0),
                    (MAIN_WHISPER_URL, 60),
                    (BACKUP_WHISPER_URL, 0)
                ]
                
                for uri, wait_time in connection_attempts:
                    if wait_time > 0:
                        logging.info(f"WHISPER: Waiting {wait_time}s to retry {uri}...")
                        await asyncio.sleep(wait_time)
                    
                    try:
                        logging.info(f"WHISPER: Connecting to {uri} for job {transcription_id}")
                        async with websockets.connect(uri, ping_interval=20, ping_timeout=2600, open_timeout=300, close_timeout=60) as remote_ws:
                            logging.info(f"WHISPER: Connected to {uri} for job {transcription_id}")
                            await remote_ws.send(json.dumps({"type": "url", "url": final_url}))
                            
                            start_time = asyncio.get_running_loop().time()
                            timeout = 3600  # 60 minutes
                            
                            async for message in remote_ws:
                                if asyncio.get_running_loop().time() - start_time > timeout:
                                    break
                                
                                try:
                                    data = json.loads(message)
                                    if "error" in data:
                                        logging.error(f"Remote server error for job {transcription_id}: {data['error']}")
                                        update_transcription_entry(transcription_id, "failed", f"Server error: {data['error']}", table_name=table_name)
                                        response_received = True; success = False; break
                                    
                                    if "message" in data:
                                        logging.info(f"Remote server status for {transcription_id}: {data['message']}")
                                        continue

                                    transcript = None
                                    if "result" in data and "hypotheses" in data["result"]:
                                        transcript = data["result"]["hypotheses"][0]["transcript"]
                                    
                                    if transcript:
                                        check_conn = sqlite3.connect(DB_PATH, timeout=20)
                                        check_c = check_conn.cursor()
                                        check_c.execute(f"SELECT status FROM {table_name} WHERE id=?", (transcription_id,))
                                        res_row = check_c.fetchone()
                                        current_status = res_row[0] if res_row else None
                                        check_conn.close()
                                        
                                        if current_status == 'cancelled':
                                            logging.info(f"Job {transcription_id} was cancelled by user. Discarding result.")
                                            response_received = True; success = True; break

                                        summary = ""
                                        if table_name != "batch_transcriptions":
                                            try: summary = summarize_text(transcript)
                                            except Exception as e: logging.error(f"Summarization error for job {transcription_id}: {e}")

                                        if update_transcription_entry(transcription_id, "completed", transcript, summary=summary, table_name=table_name):
                                            if user_email:
                                                try: set_free_trial_used(user_email)
                                                except: pass
                                                threading.Thread(target=send_transcription_result_email, args=(user_email, transcription_id, transcript, video_url, video_title, summary)).start()
                                        
                                        response_received = True; success = True; break
                                except json.JSONDecodeError: pass
                            
                            if response_received: break # Exit inner connection loop
                    except Exception as conn_err:
                        logging.warning(f"WHISPER: Connection to {uri} failed: {conn_err}")
                        if uri == BACKUP_WHISPER_URL: raise conn_err # Try next 'attempt'
                        continue
                    if response_received: break
                
                if success: break
            except Exception as e:
                logging.warning(f"Attempt {attempt+1} failed for {video_title}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                else:
                    error_msg = f"Failed after {max_retries} attempts. Last error: {e}"
                    update_transcription_entry(transcription_id, "failed", error_msg, table_name=table_name)


        if not success and not response_received: # response_received might be false if loop broke early
             update_transcription_entry(transcription_id, "failed", "Processing failed or timed out.", table_name=table_name)

    except Exception as e:
        logging.error(f"Critical error in task {transcription_id}: {e}")
        update_transcription_entry(transcription_id, "failed", f"System error: {str(e)}")
    finally:
        heartbeat_task.cancel()


async def queue_worker():
    """
    Continuous background loop that manages the persistent queue.
    """
    logging.info("Queue Worker Started (Business Grade Persistent Mode)")
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            c = conn.cursor()
            
            # 0. WATCHDOG
            try:
                c.execute("UPDATE transcription_queue SET status='failed', transcription_text='System Timeout (Stale)' WHERE status='processing' AND updated_at < datetime('now', '-60 minutes')")
                if c.rowcount > 0:
                    logging.warning(f"Watchdog: Killed {c.rowcount} stale processing jobs.")
                    conn.commit()
            except Exception as wd_err:
                 pass

            # 1. Update Stats (Count from both)
            c.execute("SELECT COUNT(*) FROM transcription_queue WHERE status='processing'")
            c1 = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM batch_transcriptions WHERE status='processing'")
            c2 = c.fetchone()[0]
            current_processing = c1 + c2
            queue_stats["current_processing"] = current_processing
            
            c.execute("SELECT COUNT(*) FROM transcription_queue WHERE status='queued'")
            w1 = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM batch_transcriptions WHERE status='queued'")
            w2 = c.fetchone()[0]
            current_waiting = w1 + w2
            queue_stats["current_waiting"] = current_waiting
            
            # 2. Check Capacity
            if current_processing >= MAX_CONCURRENT_REQUESTS:
                conn.close()
                await asyncio.sleep(2)
                continue
                
            # 3. Fetch Next Job
            job = None
            current_table = "transcription_queue"
            c.execute("SELECT id, user_id, source, title FROM transcription_queue WHERE status='queued' ORDER BY created_at ASC LIMIT 1")
            job = c.fetchone()
            
            if not job:
                current_table = "batch_transcriptions"
                c.execute("SELECT id, user_id, source, title FROM batch_transcriptions WHERE status='queued' ORDER BY created_at ASC LIMIT 1")
                job = c.fetchone()
                
            if not job:
                conn.close()
                await asyncio.sleep(2)
                continue
                
            transcription_id, user_id, video_url, video_title = job
            
            # CHECK TRIAL LIMIT BEFORE STARTING
            user = get_user_by_id(user_id)
            user_email = user[2] if user else None
            
            if user_email:
                subscribed, _, _, free_trial_count = get_user_subscription(user_email)
                if not subscribed and free_trial_count >= 10:
                     # Mark this specific queued job as failed immediately
                     c.execute(f"UPDATE {current_table} SET status='failed', transcription_text='Your free trial is completed. For transcribing more, please subscribe.', updated_at=CURRENT_TIMESTAMP WHERE id=?", (transcription_id,))
                     conn.commit()
                     conn.close()
                     logging.info(f"Refused job {transcription_id} from {current_table} due to trial limit.")
                     continue

            # Check if cancelled before starting
            c.execute(f"SELECT status FROM {current_table} WHERE id=?", (transcription_id,))
            status_check = c.fetchone()
            if status_check and status_check[0] == 'cancelled':
                conn.close()
                logging.info(f"Skipping job {transcription_id} in {current_table} as it was cancelled.")
                continue

            # IMMEDIATELY update status to 'processing' and set updated_at
            c.execute(f"UPDATE {current_table} SET status='processing', updated_at=CURRENT_TIMESTAMP WHERE id=?", (transcription_id,))
            conn.commit()
            conn.close()
            
            # Launch Task
            job_title = video_title or video_url
            asyncio.create_task(process_video_task(transcription_id, video_url, job_title, user_id, user_email, table_name=current_table))
            
            # Brief pause
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logging.error(f"Queue Worker Exception: {e}")
            await asyncio.sleep(5)

async def process_with_queue(video_url: str, video_title: str, user_id: int, user_email: str, transcription_id: int):
    """
    Legacy name used by existing endpoints. Now just adds to persistent queue.
    """
    # Simply mark as queued. The worker picks it up.
    update_transcription_entry(transcription_id, "queued", "Waiting in queue...")
    return

@app.get("/api/queue-status")
async def get_queue_status():
    """Get current queue statistics"""
    return {
        "max_concurrent": MAX_CONCURRENT_REQUESTS,
        "current_processing": queue_stats["current_processing"],
        "current_waiting": queue_stats["current_waiting"],
        "total_processed": queue_stats["total_processed"],
        "available_slots": MAX_CONCURRENT_REQUESTS - queue_stats["current_processing"]
    }

# ============= END REQUEST QUEUE SYSTEM =============


## The /api/request-transcription endpoint and all queue logic have been removed as requested.

def add_transcription_entry(user_id, source, title=None, initial_status='queued'):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("INSERT INTO transcription_queue (user_id, source, title, status) VALUES (?, ?, ?, ?)", 
              (user_id, source, title, initial_status))
    transcription_id = c.lastrowid
    conn.commit()
    conn.close()
    return transcription_id

def add_batch_transcription_entry(user_id, source, title=None, initial_status='queued', batch_id=None):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("INSERT INTO batch_transcriptions (user_id, source, title, status, batch_id) VALUES (?, ?, ?, ?, ?)", 
              (user_id, source, title, initial_status, batch_id))
    transcription_id = c.lastrowid
    conn.commit()
    conn.close()
    return transcription_id

def update_transcription_entry(transcription_id, status, text, summary=None, table_name="transcription_queue"):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    
    # User Request: Don't store full transcript in history for single video page (transcription_queue)
    # We keep it for batch_transcriptions because it's needed for the consolidation pass.
    db_text = text
    if table_name == "transcription_queue" and status == "completed":
        db_text = "" # Omit full text storage for single jobs

    # Atomic completion check: only update to 'completed' if not already 'completed'
    if status == 'completed':
        if summary:
            c.execute(f"UPDATE {table_name} SET status=?, transcription_text=?, summary=?, updated_at=CURRENT_TIMESTAMP WHERE id=? AND status != 'completed'", (status, db_text, summary, transcription_id))
        else:
            c.execute(f"UPDATE {table_name} SET status=?, transcription_text=?, updated_at=CURRENT_TIMESTAMP WHERE id=? AND status != 'completed'", (status, db_text, transcription_id))
    else:
        if summary:
            c.execute(f"UPDATE {table_name} SET status=?, transcription_text=?, summary=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, db_text, summary, transcription_id))
        else:
            c.execute(f"UPDATE {table_name} SET status=?, transcription_text=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, db_text, transcription_id))
    
    affected = c.rowcount
    conn.commit()
    conn.close()
    return affected > 0

def send_transcription_result_email(user_email, transcription_id, transcript, source, title=None, summary=None):
    """
    Unified helper to send the nicely formatted HTML transcript email.
    """
    if not user_email:
        return False
        
    subject = "Your Video Transcript"
    if title:
        subject = f"Transcript: {title}"
        
    # Clean up text for HTML
    safe_txt = transcript.replace("\n", "<br>")
    
    summary_section = ""
    if summary:
        safe_summary = summary.replace("\n", "<br>")
        summary_section = f"""
        <div style="margin-top: 25px; padding: 20px; background-color: #f0f7ff; border-radius: 8px; border: 1px solid #bae6fd;">
            <h3 style="color: #0369a1; margin-top: 0;">Simplified Key Points:</h3>
            <div style="color: #0c4a6e; font-size: 0.95rem;">{safe_summary}</div>
        </div>
        """

    html_body = f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #1f2937; max-width: 650px; margin: 0 auto; padding: 20px;">
        <div style="text-align: center; margin-bottom: 25px;">
            <h2 style="color: #4F46E5; margin-bottom: 5px;">Transcription Complete</h2>
            <p style="color: #6b7280; font-size: 0.9rem; margin-top: 0;">Your video has been successfully processed.</p>
        </div>
        
        <div style="background-color: #ffffff; padding: 25px; border-radius: 12px; border: 1px solid #e5e7eb; box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);">
            <div style="margin-bottom: 15px;">
                <p style="margin: 5px 0;"><strong>Source:</strong> <a href="{source}" style="color: #4F46E5; text-decoration: none;">{source[:60] + '...' if len(source) > 60 else source}</a></p>
                {f'<p style="margin: 5px 0;"><strong>Title:</strong> {title}</p>' if title else ''}
            </div>

            {summary_section}

            <hr style="border: 0; border-top: 1px solid #f3f4f6; margin: 25px 0;">
            
            <h3 style="color: #111827; margin-top: 0;">Full Transcript:</h3>
            <div style="background-color: #fafafa; padding: 18px; border-radius: 6px; border: 1px solid #f3f4f6; white-space: pre-wrap; font-size: 0.95rem; color: #374151;">{transcript}</div>
        </div>
        
        <div style="text-align: center; margin-top: 35px; font-size: 0.85rem; color: #9ca3af;">
            <p>Thank you for using our transcription service.</p>
            <p style="margin-top: 5px;">© {datetime.datetime.now().year} MLOPS Solution</p>
        </div>
    </body>
    </html>
    """
    
    FROM_EMAIL = "saran.d@mlopssol.com"
    APP_PASSWORD = "hsdy vntq vieu gjzu"
    
    return send_email(user_email, subject, html_body, FROM_EMAIL, APP_PASSWORD, is_html=True)

@app.get("/api/history", response_class=JSONResponse)
def get_history(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"history": []})
    
    user_id = user_row[0]
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT id, source, transcription_text, status, created_at, summary, title FROM transcription_queue WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    rows = c.fetchall()
    conn.close()
    
    history = []
    for row in rows:
        history.append({
            "id": row[0],
            "source": row[1],
            "transcription_text": row[2],
            "status": row[3],
            "created_at": row[4],
            "summary": row[5],
            "title": row[6]
        })
    return {"history": history}

@app.get("/api/check-active-transcription")
def check_active_transcription(email: str = Cookie(None)):
    if not email:
        return {"processing": False}
    
    user_row = get_user_by_email(email)
    if not user_row:
        return {"processing": False}
    
    user_id = user_row[0]
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    # Check both tables
    c.execute("SELECT id FROM transcription_queue WHERE user_id=? AND (status='processing' OR status='queued') LIMIT 1", (user_id,))
    row1 = c.fetchone()
    
    c.execute("SELECT id FROM batch_transcriptions WHERE user_id=? AND (status='processing' OR status='queued') LIMIT 1", (user_id,))
    row2 = c.fetchone()
    
    conn.close()
    
    return {"processing": bool(row1 or row2)}

@app.get("/api/batch-history", response_class=JSONResponse)
def get_batch_history(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"history": []})
    
    user_id = user_row[0]
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT id, source, transcription_text, status, created_at, summary, title, batch_id FROM batch_transcriptions WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    rows = c.fetchall()
    conn.close()
    
    history = []
    for row in rows:
        history.append({
            "id": row[0],
            "source": row[1],
            "transcription_text": row[2],
            "status": row[3],
            "created_at": row[4],
            "summary": row[5],
            "title": row[6],
            "batch_id": row[7]
        })
    return {"history": history}

class CancelRequest(BaseModel):
    type: str = "all" # Currently only "all" supported

@app.post("/api/cancel-transcription")
def cancel_transcription(request: CancelRequest, email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
         return JSONResponse({"error": "User not found"}, status_code=404)
    
    user_id = user_row[0]
    # Cancel both 'queued' and 'processing' jobs for this user in both tables
    c.execute("UPDATE transcription_queue SET status='cancelled', transcription_text='Cancelled by user', updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND status IN ('queued', 'processing')", (user_id,))
    canceled1 = c.rowcount
    
    c.execute("UPDATE batch_transcriptions SET status='cancelled', transcription_text='Cancelled by user', updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND status IN ('queued', 'processing')", (user_id,))
    canceled2 = c.rowcount
    
    conn.commit()
    conn.close()
    
    canceled_count = canceled1 + canceled2
    
    logging.info(f"User {email} cancelled {canceled_count} jobs.")
    return {"message": f"Cancelled {canceled_count} active transcriptions.", "count": canceled_count}



def update_subscription_from_stripe(email):
    subscribed, stripe_subscription_id, _, _ = get_user_subscription(email)
    if stripe_subscription_id:
        try:
            sub = stripe.Subscription.retrieve(stripe_subscription_id)
            if sub.status == "active":
                set_user_subscribed(email, stripe_subscription_id)
                return True
            else:
                # Optionally mark unsubscribed if not active
                conn = sqlite3.connect(DB_PATH, timeout=20)
                c = conn.cursor()
                c.execute("UPDATE users SET subscribed=0 WHERE email=?", (email,))
                conn.commit()
                conn.close()
                return False
        except Exception:
            return subscribed
    return subscribed

@app.post("/api/check-access", response_class=JSONResponse)
def check_access(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    subscribed, _, free_trial_used, free_trial_count = get_user_subscription(email)
    if subscribed:
        return {"access": True, "reason": "Subscribed"}
    if free_trial_count < 10:
        # Note: Trial count is incremented after successful transcription completion
        # Not here, to avoid double counting
        return {"access": True, "reason": f"Free trial: {free_trial_count}/10 used"}
    return JSONResponse({"access": False, "error": "Free trial is limited to 10 videos. Please subscribe to continue."}, status_code=403)
######################## USER PROFILE API ########################
from fastapi import Cookie

@app.get("/api/profile", response_class=JSONResponse)
def get_profile(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT name, email, subscribed, free_trial_count FROM users WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if not row:
        return JSONResponse({"error": "User not found"}, status_code=404)
    name, email, subscribed, free_trial_count = row
    return {
        "name": name,
        "email": email,
        "subscription_status": "Subscribed" if subscribed else "Not Subscribed",
        "free_trial_count": free_trial_count if free_trial_count is not None else 0
    }

class HistoryCreateRequest(BaseModel):
    source: str
    title: str = None
    status: str = None

class HistoryUpdateRequest(BaseModel):
    id: int
    status: str
    transcription_text: str = ""

@app.post("/api/history/create")
def create_history_entry(item: HistoryCreateRequest, email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"error": "User not found"}, status_code=404)
    
    user_id = user_row[0]
    # Check if a status was explicitly requested (e.g. 'processing' to claim it from worker)
    status = item.status if item.status else 'queued'
    
    transcription_id = add_transcription_entry(user_id, item.source, title=item.title, initial_status=status)
    return {"id": transcription_id}

@app.post("/api/history/update")
def update_history_entry_api(item: HistoryUpdateRequest, email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    # In a real app we might verify ownership of the history item here
    update_transcription_entry(item.id, item.status, item.transcription_text)
    return {"status": "updated"}

# --- Playlist/Batch Processing Models ---
class PlaylistRequest(BaseModel):
    playlist_url: str

class PlaylistProcessRequest(BaseModel):
    videos: list  # List of {url: str, title: str}
    page_type: str = "batch" # "single" or "batch"

class SingleVideoRequest(BaseModel):
    url: str
    title: str = None

# --- Playlist/Batch Processing Endpoints ---

@app.post("/api/extract-playlist")
async def extract_playlist(request: PlaylistRequest, email: str = Cookie(None)):
    """
    Extracts video URLs and titles from a playlist URL using yt-dlp.
    """
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    import yt_dlp
    
    ydl_opts = {
        'extract_flat': 'in_playlist',
        'quiet': True,
        'ignoreerrors': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'force_ipv4': True,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    # Try with cookies first if available
    if os.path.exists("cookies.txt"):
        ydl_opts['cookiefile'] = 'cookies.txt'
    
    # Fix for User Request: If URL is a video in a playlist (contains 'v=' and 'list='), 
    # treat it as a single video, NOT a playlist.
    clean_url = request.playlist_url
    try:
        if "youtube.com" in clean_url or "youtu.be" in clean_url:
            parsed = urlparse(clean_url)
            qs = parse_qs(parsed.query)
            if ("v" in qs and "list" in qs) or ("youtu.be" in clean_url and "list" in qs):
                # modifying request_url to remove list param
                clean_url = clean_youtube_url(clean_url)
                logging.info(f"Detected video-in-playlist. Cleaned URL to: {clean_url}")
    except: pass

    logging.info(f"Extracting videos from: {clean_url}")
    video_urls = []
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(clean_url, download=False)
            except Exception as e:
                logging.error(f"yt-dlp extract_info error: {e}")
                raise e
            
            if not info:
                 return JSONResponse({"error": "yt-dlp could not extract information."}, status_code=500)

            if 'entries' in info:
                for entry in info['entries']:
                    if not entry: continue
                    e_url = entry.get('url') or entry.get('webpage_url')
                    if not e_url and entry.get('id'):
                        e_url = f"https://www.youtube.com/watch?v={entry['id']}"
                    
                    if e_url:
                        video_urls.append({"url": e_url, "title": entry.get("title", "Unknown Title")})

            elif 'url' in info or 'webpage_url' in info:
                 v_url = info.get('url') or info.get('webpage_url')
                 video_urls.append({"url": v_url, "title": info.get("title", "Unknown Title")})
            
            logging.info(f"Extraction complete. Found {len(video_urls)} videos.")
    except Exception as e:
        logging.error(f"Playlist extraction error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
        
    return {"videos": video_urls}

async def process_single_video_background(video_url: str, video_title: str, user_id: int, user_email: str, transcription_id: int):
    """
    Helper to process a single video in the background via the queue system.
    """
    try:
        logging.info(f"Background processing: Starting job {transcription_id} for {video_title}")
        await process_with_queue(video_url, video_title, user_id, user_email, transcription_id)
    except Exception as e:
        logging.error(f"Error in background processing for {video_title}: {e}")

@app.post("/api/process-playlist-background")
async def process_playlist_background(request: PlaylistProcessRequest, email: str = Cookie(None)):
    """
    Queues multiple videos for background processing.
    """
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"error": "User not found"}, status_code=404)
    
    user_id, _, user_email = user_row
    
    subscribed, _, _, free_trial_count = get_user_subscription(email)
    if not subscribed and free_trial_count >= 10:
        return JSONResponse({"error": "Free trial limit reached"}, status_code=403)

    video_count = len(request.videos)
    logging.info(f"Queueing batch of {video_count} videos for {user_email}")
    
    # Use current timestamp as batch_id to group these requests
    batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    
    for video in request.videos:
        v_url = video.get("url", "")
        if not v_url: continue
        v_title = video.get("title") or v_url
        
        if request.page_type == "single":
            # Add to single-video queue so it shows up in main page history
            tid = add_transcription_entry(user_id, v_url, title=v_title)
        else:
            # Add to batch table for multi-video page
            tid = add_batch_transcription_entry(user_id, v_url, title=v_title, batch_id=batch_id)
        # Background worker now picks it up automatically
    
    return {
        "status": "processing",
        "message": f"Successfully queued {video_count} videos. Results will be sent to your email.",
        "video_count": video_count
    }

@app.post("/api/process-single-video")
async def process_single_video(request: SingleVideoRequest, email: str = Cookie(None)):
    """
    Process a single video via background queue.
    """
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"error": "User not found"}, status_code=404)
    
    user_id, _, user_email = user_row
    subscribed, _, _, free_trial_count = get_user_subscription(email)
    
    if not subscribed and free_trial_count >= 10:
        return JSONResponse({"error": "Free trial limit reached"}, status_code=403)
    
    # Clean URL to prevent accidental playlist processing
    request.url = clean_youtube_url(request.url)
    
    v_title = request.title or request.url
    tid = add_transcription_entry(user_id, request.url, title=v_title)
    asyncio.create_task(process_single_video_background(request.url, v_title, user_id, user_email, tid))
    
    return {
        "status": "queued",
        "message": "Video queued for processing.",
        "transcription_id": tid
    }

from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key="abc123")

# Serve static files folder (if needed)
app.mount("/files", StaticFiles(directory=os.path.dirname(__file__), html=True), name="static")
app.mount("/static", StaticFiles(directory="."), name="static")

from fastapi import Request
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config

# Google OAuth Setup
# ALLOW HTTP FOR LOCALHOST (Critical for local testing)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

config = Config(environ={
    "GOOGLE_CLIENT_ID": "43267326088-hojkf1n0373rl2rsv23dvv87t2gi60sg.apps.googleusercontent.com",
    "GOOGLE_CLIENT_SECRET": "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"
})

oauth = OAuth(config)

google = oauth.register(
    name="google",
    client_id="43267326088-caim6kq5fimb1kpohqcg14dem7ge5gd3.apps.googleusercontent.com",
    client_secret="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",
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

######################## STRIPE PAYMENT ########################
stripe.api_key = "sk_test_51SWxcx3FrGPVy3Ub2SC96WVgzEB5pKZMvDKupBfmtpHRlxAM1H5m9NLUmwR0m9Ks3Cm9ILlTYbbXrz8sHjLUrOO200jGhBTorx"       # Your Stripe secret key
DOMAIN = "https://audio2text.mlopssol.com"      # Production domain

@app.post("/create-checkout-session")
async def create_checkout_session(request: Request):
    domain = str(request.base_url).rstrip('/')
    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[
                {
                    "price": "price_1SbyWM3FrGPVy3Ub10Bb5J6c",  # Your price ID
                    "quantity": 1
                }
            ],
            success_url=f"{domain}/payment-success",
            cancel_url=f"{domain}/payment-cancel",
        )
        return JSONResponse({"sessionId": checkout_session.id})
    except Exception as e:
        return JSONResponse({"error": str(e)})

# Remove previous Stripe logic and add new endpoint for $3 plan
@app.post("/create-checkout-session-3dollar")
async def create_checkout_session_3dollar(request: Request):
    domain = str(request.base_url).rstrip('/')
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": "price_1SbyWM3FrGPVy3Ub10Bb5J6c",
                "quantity": 1,
            }],
            success_url=f"{domain}/billing?success=true",
            cancel_url=f"{domain}/billing?canceled=true",
        )
        return {"sessionId": session.id}
    except Exception as e:
        return {"error": str(e)}

@app.post("/create-checkout-session-3inr")
async def create_checkout_session_3inr(request: Request):
    domain = str(request.base_url).rstrip('/')
    import stripe
    stripe.api_key = "sk_test_51SWxcx3FrGPVy3Ub2SC96WVgzEB5pKZMvDKupBfmtpHRlxAM1H5m9NLUmwR0m9Ks3Cm9ILlTYbbXrz8sHjLUrOO200jGhBTorx"
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": "price_1SckQS3FrGPVy3Ubeh6OGMG2",  # INR price ID
                "quantity": 1,
            }],
            success_url=f"{domain}/billing?success=true",
            cancel_url=f"{domain}/billing?canceled=true",
        )
        return {"sessionId": session.id}
    except Exception as e:
        return {"error": str(e)}

@app.post("/create-checkout-session-3dollar-yearly")
async def create_checkout_session_3dollar_yearly(request: Request):
    domain = str(request.base_url).rstrip('/')
    import stripe
    stripe.api_key = "sk_test_51SWxcx3FrGPVy3Ub2SC96WVgzEB5pKZMvDKupBfmtpHRlxAM1H5m9NLUmwR0m9Ks3Cm9ILlTYbbXrz8sHjLUrOO200jGhBTorx"
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": "price_1SckoH3FrGPVy3Uboq7ahDeY",  # Yearly USD price ID
                "quantity": 1,
            }],
            success_url=f"{domain}/billing?success=true",
            cancel_url=f"{domain}/billing?canceled=true",
        )
        return {"sessionId": session.id}
    except Exception as e:
        return {"error": str(e)}

@app.post("/create-checkout-session-3inr-yearly")
async def create_checkout_session_3inr_yearly(request: Request):
    domain = str(request.base_url).rstrip('/')
    import stripe
    stripe.api_key = "sk_test_51SWxcx3FrGPVy3Ub2SC96WVgzEB5pKZMvDKupBfmtpHRlxAM1H5m9NLUmwR0m9Ks3Cm9ILlTYbbXrz8sHjLUrOO200jGhBTorx"
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": "price_1Sckwc3FrGPVy3UbEWvoIEsG",  # Yearly INR price ID
                "quantity": 1,
            }],
            success_url=f"{domain}/billing?success=true",
            cancel_url=f"{domain}/billing?canceled=true",
        )
        return {"sessionId": session.id}
    except Exception as e:
        return {"error": str(e)}

@app.get("/payment-success", response_class=HTMLResponse)
def success(request: Request):
    # Get user email from cookie
    email = request.cookies.get("email")
    stripe_subscription_id = request.query_params.get("subscription_id")
    if email:
        set_user_subscribed(email, stripe_subscription_id)
    return """
    <h2>Payment Successful 🎉</h2>
    <script>
    localStorage.setItem("subscribed", "true");
    window.location.href = "/";
    </script>
    """

@app.get("/payment-cancel", response_class=HTMLResponse)
def cancel():
    return '''<h2>Payment Cancelled ❌</h2>
    <a href="/" style="display:inline-block;margin-top:18px;padding:10px 22px;background:#6366f1;color:#fff;border-radius:7px;text-decoration:none;font-weight:600;">Go back to Speech to Text</a>'''
#################################################################
@app.get("/login/google")
async def login_google(request: Request):
    redirect_uri = str(request.url_for('auth_google_callback'))
    print(f"--- DEBUG: Redirect URI sent to Google: {redirect_uri} ---")
    return await google.authorize_redirect(request, redirect_uri)

@app.get("/auth/google/callback")
async def auth_google_callback(request: Request):
    try:
        token = await google.authorize_access_token(request)
    except Exception as e:
        print(f"--- DEBUG: Token Exchange Failed: {e} ---")
        return JSONResponse({"error": f"Token Exchange Failed: {e}"}, status_code=400)
        
    user = token.get("userinfo")
    if not user:
        print("--- DEBUG: No userinfo in token ---")
        return JSONResponse({"error": "No user info found"}, status_code=400)

    email = user["email"]
    name = user["name"]
    print(f"--- DEBUG: Logged in user: {email} ---")

    # Save user to database
    save_user(email, name)
    # Always check DB for subscription status and trial usage
    subscribed, _, free_trial_used, _ = get_user_subscription(email)
    stripe_verified = update_subscription_from_stripe(email)
    final_subscribed = subscribed or stripe_verified

    # store session in cookies
    response = RedirectResponse("/")
    response.set_cookie("loggedIn", "true")
    response.set_cookie("email", email)
    response.set_cookie("name", name)
    response.set_cookie("subscribed", str(int(final_subscribed)))
    # Set free trial cookie from DB
    response.set_cookie("free_trial_used", str(int(free_trial_used)))
    # Reset free trial cookie if subscribed
    if final_subscribed:
        response.set_cookie("free_trial_used", "0")
    return response

######################## WEBSOCKET TRANSCRIPTION ########################
from fastapi import WebSocket, WebSocketDisconnect
import asyncio
import json
import websockets

# -----------------------------------------------------------------
# Persistent WebSocket – handles transcription and then stays alive
# -----------------------------------------------------------------
@app.websocket("/ws")

async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()

    # Auto-fail stale jobs > 5 mins (Self-Healing)
    try:
        conn = sqlite3.connect(DB_PATH, timeout=20)
        c = conn.cursor()
        # Using -30 minutes to clear likely stuck jobs. 
        c.execute("UPDATE transcription_queue SET status='failed', transcription_text='Stale/Timeout' WHERE status='processing' AND created_at < datetime('now', '-60 minutes')")
        if c.rowcount > 0:
            logging.info(f"Auto-cleaned {c.rowcount} stale jobs on connect.")
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Cleanup error: {e}")
    
    # Identify User
    user_email = client_ws.cookies.get("email")
    logging.info(f"WS Connection: Email cookie: {user_email}")
    user_id = None
    if user_email:
        u_row = get_user_by_email(user_email)
        if u_row:
            user_id = u_row[0]

    current_source = "Unknown Source"
    current_transcription_id = None
    data_sent_to_remote = False  # Track if we've sent data to remote (for background processing)
    
    # Proxy Connection to External Whisper Model with Fallback Logic
    try:
        connection_attempts = [
            (MAIN_WHISPER_URL, 0),    # Try Main
            (MAIN_WHISPER_URL, 60),   # Wait 60s, Retry Main
            (BACKUP_WHISPER_URL, 0)   # Backup
        ]
        
        for uri, wait_time in connection_attempts:
            if wait_time > 0:
                logging.info(f"WS Proxy: Waiting {wait_time}s to retry {uri}...")
                await asyncio.sleep(wait_time)
            
            try:
                logging.info(f"WS Proxy: Connecting to {uri}")
                async with websockets.connect(uri, ping_interval=None, ping_timeout=None, open_timeout=300, close_timeout=60) as remote_ws:
                    logging.info(f"WS Proxy: Successfully connected to {uri}")
                    
                    # --- Task: Forward Client -> Remote ---
                    async def forward_client_to_remote():
                        nonlocal current_source, current_transcription_id, data_sent_to_remote
                        try:
                            while True:
                                data = await client_ws.receive()
                                if "text" in data:
                                    text = data["text"]
                                    try:
                                        json_data = json.loads(text)
                                        if json_data.get("type") == "url":
                                            current_source = clean_youtube_url(json_data.get("url"))
                                            json_data['url'] = current_source
                                            text = json.dumps(json_data)
                                            data_sent_to_remote = True
                                            if user_id and not current_transcription_id:
                                                check_conn = sqlite3.connect(DB_PATH); check_c = check_conn.cursor()
                                                check_c.execute("SELECT id, status FROM transcription_queue WHERE user_id=? AND status IN ('queued', 'processing') ORDER BY created_at DESC LIMIT 1", (user_id,))
                                                res = check_c.fetchone()
                                                if res:
                                                    current_transcription_id, job_status = res
                                                    if job_status == 'queued':
                                                        check_c.execute("UPDATE transcription_queue SET status='processing' WHERE id=?", (current_transcription_id,))
                                                        check_conn.commit()
                                                check_conn.close()

                                            if user_email:
                                                sub, _, _, trial = get_user_subscription(user_email)
                                                if not sub and trial >= 10:
                                                    await client_ws.send_text(json.dumps({"error": "Trial limit reached"}))
                                                    await client_ws.close(); return
                                        
                                        elif json_data.get("type") == "file_meta":
                                            current_source = json_data.get("filename")
                                            data_sent_to_remote = True
                                            provided_tid = json_data.get("transcription_id")
                                            if provided_tid: current_transcription_id = provided_tid
                                            elif user_id and not current_transcription_id:
                                                check_conn = sqlite3.connect(DB_PATH); check_c = check_conn.cursor()
                                                check_c.execute("SELECT id, status FROM transcription_queue WHERE user_id=? AND status IN ('queued', 'processing') ORDER BY created_at DESC LIMIT 1", (user_id,))
                                                res = check_c.fetchone()
                                                if res:
                                                    current_transcription_id, job_status = res
                                                    if job_status == 'queued':
                                                        check_c.execute("UPDATE transcription_queue SET status='processing' WHERE id=?", (current_transcription_id,))
                                                        check_conn.commit()
                                                check_conn.close()
                                    except:
                                        if text == "EOF": data_sent_to_remote = True
                                    await remote_ws.send(text)
                                elif "bytes" in data:
                                    await remote_ws.send(data["bytes"])
                                    data_sent_to_remote = True
                        except: pass

                    # --- Task: Forward Remote -> Client ---
                    async def listen_remote_and_respond():
                        nonlocal current_transcription_id
                        try:
                            async for message in remote_ws:
                                try:
                                    data = json.loads(message)
                                    transcript = None
                                    if "result" in data and "hypotheses" in data["result"]:
                                        transcript = data["result"]["hypotheses"][0]["transcript"]
                                    
                                    if transcript:
                                        if current_transcription_id:
                                            tid = current_transcription_id; current_transcription_id = None
                                            summary = ""; 
                                            try: summary = summarize_text(transcript)
                                            except: pass
                                            if update_transcription_entry(tid, "completed", transcript, summary=summary):
                                                if user_email:
                                                    try: set_free_trial_used(user_email)
                                                    except: pass
                                                    threading.Thread(target=send_transcription_result_email, args=(user_email, tid, transcript, current_source, None, summary)).start()
                                            if summary: 
                                                try: data["result"]["summary"] = summary; message = json.dumps(data)
                                                except: pass
                                except: pass
                                try: await client_ws.send_text(message)
                                except: pass
                        except: pass

                    async def keep_client_alive():
                        while True:
                            await asyncio.sleep(15)
                            try: await client_ws.send_text(json.dumps({"type": "ping"}))
                            except: break

                    async def keep_db_alive():
                        while True:
                            await asyncio.sleep(60)
                            if current_transcription_id:
                                try:
                                    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                                    c.execute("UPDATE transcription_queue SET updated_at=CURRENT_TIMESTAMP WHERE id=?", (current_transcription_id,))
                                    conn.commit(); conn.close()
                                except: pass

                    client_task = asyncio.create_task(forward_client_to_remote())
                    remote_task = asyncio.create_task(listen_remote_and_respond())
                    keepalive_task = asyncio.create_task(keep_client_alive())
                    db_heartbeat_task = asyncio.create_task(keep_db_alive())
                    
                    done, _ = await asyncio.wait([client_task, remote_task], return_when=asyncio.FIRST_COMPLETED)
                    keepalive_task.cancel(); db_heartbeat_task.cancel()
                    
                    if client_task in done and data_sent_to_remote:
                         try: await asyncio.wait_for(remote_task, timeout=3600) 
                         except: pass

                # If we got here without exception and remote_ws finished correctly, we are done
                break
            except Exception as e:
                logging.warning(f"WS Proxy: Attempt with {uri} failed: {e}")
                if uri == BACKUP_WHISPER_URL: raise e
                continue


    except Exception as e:
        logging.error(f"Proxy global error: {e}")
        # Update history if we have a transcription ID
        if current_transcription_id:
            try:
                update_transcription_entry(current_transcription_id, "failed", f"Connection error: {str(e)}")
            except Exception as update_err:
                logging.error(f"Failed to update history on error: {update_err}")
        try:
            # Try to notify client of global error
            await client_ws.send_text(json.dumps({"error": "Internal server error during proxy."}))
            await client_ws.close()
        except:
            pass



@app.get("/favicon.ico", include_in_schema=False)
def serve_favicon():
    return FileResponse(os.path.join(os.path.dirname(__file__), "favicon.ico"), media_type="image/x-icon")

@app.get("/robots.txt")
def serve_robots():
    return FileResponse(os.path.join(os.path.dirname(__file__), "robots.txt"))

@app.get("/sitemap.xml")
def serve_sitemap():
    return FileResponse(os.path.join(os.path.dirname(__file__), "sitemap.xml"), media_type="application/xml")

######################## FRONTEND PAGE #########################
@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    html_path = os.path.join(os.path.dirname(__file__), "video-speech-to-text.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: video-speech-to-text.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())

@app.get("/amp/", response_class=HTMLResponse)
@app.get("/amp", response_class=HTMLResponse)
def serve_frontend_amp():
    html_path = os.path.join(os.path.dirname(__file__), "video-speech-to-text-amp.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: video-speech-to-text-amp.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())
################################################################

@app.get("/billing", response_class=HTMLResponse)
def serve_billing():
    html_path = os.path.join(os.path.dirname(__file__), "billing.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: billing.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())

@app.get("/multi-video-transcription", response_class=HTMLResponse)
def serve_multi_video_transcription():
    html_path = os.path.join(os.path.dirname(__file__), "multi-video-transcription.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: multi-video-transcription.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())

@app.get("/amp/multi-video-transcription", response_class=HTMLResponse)
def serve_multi_video_transcription_amp():
    html_path = os.path.join(os.path.dirname(__file__), "multi-video-transcription-amp.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: multi-video-transcription-amp.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())

@app.get("/multi-transcription", response_class=HTMLResponse)
def serve_multi_transcription():
    return serve_multi_video_transcription()

@app.get("/bulk-results", response_class=HTMLResponse)
def serve_bulk_results_page():
    html_path = os.path.join(os.path.dirname(__file__), "bulk-results.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: bulk-results.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())


@app.get("/contact", response_class=HTMLResponse)
def serve_contact_page():
    html_path = os.path.join(os.path.dirname(__file__), "contact.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Error: contact.html not found.</h1>", status_code=404)
    return HTMLResponse(open(html_path, "r", encoding="utf-8").read())

class ContactRequest(BaseModel):
    name: str
    email: str
    subject: str
    message: str

@app.post("/api/contact")
async def submit_contact_form(
    name: str = Form(...),
    email: Optional[str] = Form(None),
    subject: str = Form(...),
    message: str = Form(...),
    attachment: UploadFile = None
):
    # Prepare email content
    # In a real app, this would send an email to support@yourdomain.com
    # For now we'll just log it or simulate sending to the admin
    
    # If email wasn't in form, try to find it? 
    # Actually, the frontend JS handles sending whatever it finds. 
    # But if frontend sends empty string, we should handle that.
    user_email = email if email and email.strip() else "Anonymous/Guest"
    
    admin_email = "saran.d@mlopssol.com" # Using your email as admin/support
    email_subject = f"Audio to Text: {subject}"
    email_body = f"""
    New message from Contact Form:
    
    Name: {name}
    Email: {user_email}
    Subject: {subject}
    
    Message:
    {message}
    """
    
    email_attachment = None
    if attachment:
        content = await attachment.read()
        email_attachment = (attachment.filename, content, attachment.content_type)
        email_body += f"\n\n[Attachment included: {attachment.filename}]"
    
    try:
        # Use existing send_email function
        # Hardcoding credentials again as seen in other endpoints for consistency
        FROM_EMAIL = "saran.d@mlopssol.com"
        APP_PASSWORD = "hsdy vntq vieu gjzu"
        
        send_email(admin_email, email_subject, email_body, FROM_EMAIL, APP_PASSWORD, attachment=email_attachment)
        return {"status": "success", "message": "Message sent successfully"}
    except Exception as e:
        logging.error(f"Failed to send contact email: {e}")
        # Return success anyway to user so they don't see error for backend issue if it's just email config
        return {"status": "success", "message": "Message received"}

@app.post("/api/consolidate-topic-wise")
async def consolidate_topic_wise(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    try:
        user_row = get_user_by_email(email)
        if not user_row:
            return JSONResponse({"error": "User not found"}, status_code=404)
        
        user_id = user_row[0]
        conn = sqlite3.connect(DB_PATH, timeout=20)
        c = conn.cursor()
        
        # Identify the latest batch_id for this user that has at least one completed video
        c.execute("SELECT batch_id FROM batch_transcriptions WHERE user_id=? AND status='completed' ORDER BY created_at DESC LIMIT 1", (user_id,))
        row = c.fetchone()
        
        if not row or not row[0]:
            conn.close()
            return JSONResponse({"error": "No completed batch transcriptions found to consolidate."}, status_code=400)

        latest_batch_id = row[0]
        logging.info(f"Consolidating batch {latest_batch_id} for user {user_id}")

        # Get all completed transcriptions FROM THE LATEST BATCH ONLY, including their sources
        c.execute("SELECT source, transcription_text FROM batch_transcriptions WHERE user_id=? AND status='completed' AND batch_id=? ORDER BY created_at ASC", (user_id, latest_batch_id))
        rows = c.fetchall()
        
        if not rows:
            conn.close()
            return JSONResponse({"error": "No completed transcriptions found in the latest batch."}, status_code=400)
        
        urls = [r[0] for r in rows]
        # Include URLs in the combined text for the LLM to potentially reference, 
        # and definitely to keep them "kept" as requested.
        combined_segments = []
        for source, text in rows:
            combined_segments.append(f"SOURCE URL: {source}\nTRANSCRIPTION:\n{text}")
        
        combined_text = "\n\n--- NEXT VIDEO ---\n\n".join(combined_segments)
        
        # Use the specific topic-wise synthesis function
        consolidated_text = generate_topic_wise_consolidation(combined_text)
        
        # Construct the full transcription result section (without repeating individual source links)
        full_transcription_section = "\n\n" + "-"*30 + "\n" + "COMPLETE TRANSCRIPTIONS FROM ALL SOURCES\n" + "-"*30 + "\n\n"
        for i, (source, text) in enumerate(rows, 1):
            full_transcription_section += f"### Video {i} Transcription:\n\n{text}\n\n"
            full_transcription_section += "---\n\n"

        # Display source URLs only once at the very bottom
        urls_list_str = "\n\nSources used in this synthesis:\n" + "\n".join([f"- {url}" for url in urls])
        
        # Combine the unique AI synthesis with the full raw transcriptions
        final_output = consolidated_text + full_transcription_section + urls_list_str
        
        # STORE the bulk transcription result in the database
        try:
            c.execute("""
                INSERT INTO bulk_results (user_id, batch_id, topic_wise_result, urls) 
                VALUES (?, ?, ?, ?)
            """, (user_id, latest_batch_id, final_output, ",".join(urls)))
            conn.commit()
        except Exception as e:
            logging.error(f"Error storing bulk result: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        return {"consolidated_text": final_output}

    except Exception as e:
        logging.error(f"Consolidation API error: {e}")
        return JSONResponse({"error": f"Server error: {str(e)}"}, status_code=500)


######################## WHISPER MODEL ########################

# Function to mark free trial as used (global scope)
def set_free_trial_used(email):
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("UPDATE users SET free_trial_count = COALESCE(free_trial_count, 0) + 1 WHERE email=?", (email,))
    conn.commit()
    conn.close()
################################################################

# --- Admin Free Access ---
ADMIN_EMAILS = ["admin@yourdomain.com"]  # Add your admin email(s) here

@app.get("/admin2", response_class=HTMLResponse)
def admin_login():
    # For demo: set admin session cookies
    response = HTMLResponse("""
    <h2>Admin Access Granted</h2>
    <script>
    document.cookie = "loggedIn=true";
    document.cookie = "email=admin@yourdomain.com";
    document.cookie = "name=Admin";
    document.cookie = "subscribed=1";
    document.cookie = "free_trial_used=0";
    window.location.href = "/";
    </script>
    """)
    return response

@app.get("/api/bulk-results")
def get_bulk_results(email: str = Cookie(None)):
    if not email:
        return JSONResponse({"error": "Not logged in"}, status_code=401)
    
    user_row = get_user_by_email(email)
    if not user_row:
        return JSONResponse({"results": []})
    
    user_id = user_row[0]
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT id, batch_id, topic_wise_result, urls, created_at FROM bulk_results WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    rows = c.fetchall()
    conn.close()
    
    results = []
    for row in rows:
        results.append({
            "id": row[0],
            "batch_id": row[1],
            "content": row[2],
            "urls": row[3],
            "created_at": row[4]
        })
    return {"results": results}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify your frontend URL(s)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- EMAIL SENDING LOGIC SETUP ---


# Example usage:
# send_email(
#     to_email="recipient@example.com",
#     subject="Test Email",
#     body="This is a test email.",
#     from_email="your_email@gmail.com",
#     app_password="your_app_password"
# )

# --- Transcription endpoint (example, add after Whisper logic) ---
from fastapi import UploadFile, Form

@app.post("/api/transcribe")
def transcribe_api(
    video_url: str = Form(None),
    video_file: UploadFile = None,
    email: str = Cookie(None)
):
    # ...existing transcription logic...
    # After transcription completes:
    transcript = "...transcript text..."  # Replace with actual transcript
    # Fetch user email from DB if not provided
    if not email:
        return JSONResponse({"error": "User email not found in cookies."}, status_code=400)
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT email FROM users WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    if not row:
        return JSONResponse({"error": "User not found."}, status_code=404)
    user_email = row[0]
    # Send transcript email
    subject = "Your Video Transcript"
    body = f"Hello,\n\nHere is your transcript:\n\n{transcript}\n\nThank you for using our service!"
    # Set your email and app password here
    FROM_EMAIL = "saran.d@mlopssol.com"
    APP_PASSWORD = "hsdy vntq vieu gjzu"
    send_ok = send_email(user_email, subject, body, FROM_EMAIL, APP_PASSWORD)
    if send_ok:
        return JSONResponse({"status": "Email sent with transcript."})
    else:
        return JSONResponse({"error": "Failed to send email."}, status_code=500)

from pydantic import BaseModel

class TranscriptEmailRequest(BaseModel):
    transcript: str
    source: str
    type: str  # "link" or "file"

@app.post("/api/send-transcript-email")
def send_transcript_email(
    request: TranscriptEmailRequest,
    email: str = Cookie(None)
):
    if not email:
        return JSONResponse({"error": "User email not found in cookies."}, status_code=400)
    
    conn = sqlite3.connect(DB_PATH, timeout=20)
    c = conn.cursor()
    c.execute("SELECT email FROM users WHERE email=?", (email,))
    row = c.fetchone()
    conn.close()
    
    if not row:
        return JSONResponse({"error": "User not found."}, status_code=404)
    
    user_email = row[0]
    subject = "Your Video Transcript"
    
    
    # Clean up text for HTML
    safe_txt = request.transcript.replace("\n", "<br>")
    
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="text-align: center; margin-bottom: 20px;">
            <h2 style="color: #4F46E5;">Video Transcription Complete</h2>
        </div>
        <div style="background-color: #f9fafb; padding: 20px; border-radius: 8px; border: 1px solid #e5e7eb;">
            <p><strong>Source:</strong> <a href="{request.source}" style="color: #4F46E5;">{request.source}</a></p>
            <hr style="border: 0; border-top: 1px solid #e5e7eb; margin: 20px 0;">
            <h3 style="color: #111;">Transcript:</h3>
            <div style="background-color: #ffffff; padding: 15px; border-radius: 4px; border: 1px solid #e5e7eb; white-space: pre-wrap;">{request.transcript}</div>
        </div>
        <div style="text-align: center; margin-top: 30px; font-size: 0.9em; color: #6b7280;">
            <p>Thank you for using our service.</p>
        </div>
    </body>
    </html>
    """
    
    # Use the credentials present in the file
    FROM_EMAIL = "saran.d@mlopssol.com"
    APP_PASSWORD = "hsdy vntq vieu gjzu"
    
    send_ok = send_email(user_email, subject, html_body, FROM_EMAIL, APP_PASSWORD, is_html=True)
    
    if send_ok:
        return JSONResponse({"status": "Email sent with transcript."})
    else:
        return JSONResponse({"error": "Failed to send email."}, status_code=500)

@app.on_event("startup")
async def start_background_tasks():
    # Start the persistent queue worker
    asyncio.create_task(queue_worker())
    logging.info("Background queue worker launched.")