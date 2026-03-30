import sqlite3
conn = sqlite3.connect('jobs.db')
try:
    conn.execute("ALTER TABLE settings ADD COLUMN category TEXT DEFAULT 'Technology'")
    conn.commit()
    print("Added 'category' column")
except Exception as e:
    print(f"Already exists or error: {e}")
# Also set a default value for existing rows
conn.execute("UPDATE settings SET category='Technology' WHERE category IS NULL")
conn.commit()
conn.close()
print("Done")
