"""
Script to add @require_admin_auth decorator to all admin routes
"""

import re

# Read the app.py file
with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Pattern to find admin routes that don't have the decorator
pattern = r'(@app\.route\("/admin/[^"]+",.*?\))\ndef '

# Find all matches
matches = list(re.finditer(pattern, content, re.DOTALL))

print(f"Found {len(matches)} admin routes")

# Add decorator to each route
new_content = content
for match in reversed(matches):  # Reverse to maintain positions
    route_decorator = match.group(1)
    # Check if already has require_admin_auth
    check_start = max(0, match.start() - 100)
    preceding_text = content[check_start:match.start()]
    
    if '@require_admin_auth' not in preceding_text:
        # Insert the decorator after the route decorator
        insert_pos = match.end(1)
        new_content = new_content[:insert_pos] + '\n@require_admin_auth' + new_content[insert_pos:]
        print(f"Added decorator to: {route_decorator}")

# Write back
with open('app.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("\n✅ Admin routes secured!")
