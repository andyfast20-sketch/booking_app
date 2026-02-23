import os
import re
from pathlib import Path

def optimize_svg(file_path):
    """Remove unnecessary whitespace and comments from SVG files"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_size = len(content)
    
    # Remove comments
    content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
    
    # Remove unnecessary whitespace between tags
    content = re.sub(r'>\s+<', '><', content)
    
    # Remove leading/trailing whitespace
    content = content.strip()
    
    # Minify: remove newlines and extra spaces
    content = re.sub(r'\s+', ' ', content)
    
    new_size = len(content)
    savings = original_size - new_size
    savings_pct = (savings / original_size * 100) if original_size > 0 else 0
    
    # Save optimized version
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return original_size, new_size, savings, savings_pct

def main():
    images_dir = Path('c:/booking_app/static/images')
    
    print("Optimizing SVG images...\n")
    
    total_original = 0
    total_new = 0
    
    for svg_file in images_dir.glob('*.svg'):
        orig, new, savings, pct = optimize_svg(svg_file)
        total_original += orig
        total_new += new
        
        print(f"{svg_file.name}")
        print(f"  Before: {orig/1024:.2f} KB")
        print(f"  After:  {new/1024:.2f} KB")
        print(f"  Saved:  {savings/1024:.2f} KB ({pct:.1f}%)\n")
    
    total_savings = total_original - total_new
    total_pct = (total_savings / total_original * 100) if total_original > 0 else 0
    
    print(f"Total optimization:")
    print(f"  Before: {total_original/1024:.2f} KB")
    print(f"  After:  {total_new/1024:.2f} KB")
    print(f"  Saved:  {total_savings/1024:.2f} KB ({total_pct:.1f}%)")
    print(f"\n✅ Images optimized! They should load much faster now.")

if __name__ == '__main__':
    main()
