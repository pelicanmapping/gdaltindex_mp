# gdaltindex_mp

Process large lists of geospatial files using `gdaltindex` in parallel batches, then merge results into a single FlatGeoBuf file.

## Requirements

- GDAL tools (`gdaltindex`, `ogrmerge.py`)
- Bash 4.0+
- Standard Unix utilities (`xargs`, `split`, `find`)

### Installing GDAL

**Ubuntu/Debian:**
```bash
sudo apt install gdal-bin python3-gdal
```

**Fedora:**
```bash
sudo dnf install gdal gdal-python-tools
```

**macOS (Homebrew):**
```bash
brew install gdal
```

## Usage

```bash
./gdaltindex_mp.sh -i INPUT_FILE -o OUTPUT_FILE [OPTIONS]
```

### Required Arguments

| Argument | Description |
|----------|-------------|
| `-i, --input FILE` | Text file containing list of geospatial files (one per line) |
| `-o, --output FILE` | Output FlatGeoBuf file name (e.g., `output.fgb`) |

### Optional Arguments

| Argument | Description |
|----------|-------------|
| `-b, --batch-size N` | Number of files per batch (default: 1000) |
| `-j, --jobs N` | Number of parallel jobs (default: all CPUs) |
| `-t, --temp-dir DIR` | Directory for temporary files (default: auto-created) |
| `-k, --keep-temp` | Keep temporary files after completion |
| `-h, --help` | Show help message |

## Examples

### Basic usage

```bash
./gdaltindex_mp.sh -i file_list.txt -o index.fgb
```

### Custom batch size and parallel jobs

```bash
./gdaltindex_mp.sh -i file_list.txt -o index.fgb -b 500 -j 8
```

### Keep temporary files for debugging

```bash
./gdaltindex_mp.sh -i file_list.txt -o index.fgb -k -t /tmp/my_temp
```

## Input File Format

The input file should contain one geospatial file path per line:

```
/path/to/file1.tif
/path/to/file2.tif
/path/to/file3.tif
```

Generate a file list with `find`:

```bash
find /data/imagery -name "*.tif" > file_list.txt
```

## How It Works

1. Splits the input file list into batches
2. Runs `gdaltindex` in parallel on each batch, creating intermediate GeoPackage files
3. Merges all GeoPackages into a single FlatGeoBuf output file
4. Cleans up temporary files (unless `-k` is specified)
