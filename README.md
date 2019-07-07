# bigdata-edu
Processing, analysis and visualization of education data in Brazil with distributed frameworks.

### Decompress zip files in dictories
find . -name "*.zip" | while read filename; do unzip -o -d "`dirname "$filename"`" "$filename"; done;

files where moved to directories with their year as the dict name
