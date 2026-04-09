#!/bin/bash

# Go to the FileShare folder
cd "$(dirname "$0")"

# Auto-generate files.json from whatever is in the files/ folder
echo "[" > files.json
first=true
for f in files/*; do
  filename=$(basename "$f")
  # Skip hidden files and placeholder
  [[ "$filename" == .* ]] && continue
  [[ "$filename" == "example_script.txt" ]] && continue

  date=$(date +%Y-%m-%d)

  if [ "$first" = true ]; then
    first=false
  else
    echo "," >> files.json
  fi

  echo "  {" >> files.json
  echo "    \"name\": \"$filename\"," >> files.json
  echo "    \"label\": \"$filename\"," >> files.json
  echo "    \"description\": \"\"," >> files.json
  echo "    \"date\": \"$date\"" >> files.json
  echo -n "  }" >> files.json
done
echo "" >> files.json
echo "]" >> files.json

# Push to GitHub
git add .
git commit -m "Update files $(date +%Y-%m-%d)"
git push

echo ""
echo "Done! Your site will update in about 30 seconds."
echo "Visit: https://matheodabo.github.io/file-drop/"
echo ""
read -p "Press Enter to close..."
