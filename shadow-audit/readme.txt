# Shadow Audit - TIG Device Active Usage Checker

## Purpose
Identifies TIG devices with active cellular data usage by cross-referencing:
- OEM Historical Usage.xlsx (data usage records)
- devices.csv (device registry with ICCID/DSN/type mappings)

## Prerequisites
- Python 3.x
- pandas library: pip install pandas
- openpyxl library: pip install openpyxl

## Files Required
- OEM Historical Usage.xlsx (in shadow-audit/ directory)
- devices.csv (in shadow-audit/ directory)

## Running the Script
```bash
python analyze.py
```

The script will:
1. Load OEM Historical Usage data
2. Filter to units with Cycle-to-date Data Usage > 0
3. Join with devices.csv on ICCID
4. Filter to TIG device type only
5. Display count of matches and prompt to continue

## Output
Console output shows total count of active TIG devices. No files are generated.
The script will pause after displaying the count and wait for user input (press Enter to continue).

## Example Output
```
============================================================
SHADOW AUDIT: TIG Devices with Active Data Usage
============================================================

TIG devices with Cycle-to-date Data Usage > 0: 170424

Press Enter to continue...
```

## Troubleshooting

**Error: Missing file**
- Ensure both OEM Historical Usage.xlsx and devices.csv are in the shadow-audit/ directory
- Check file names match exactly (case-sensitive on Linux/Mac)

**Error: No module named pandas/openpyxl**
- Run: pip install pandas openpyxl
