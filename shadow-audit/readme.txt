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
- vehicles.csv (in shadow-audit/ directory) - Not Communicating vehicles from PACCAR

## Running the Script
```bash
python analyze.py
```

The script will:
1. Load OEM Historical Usage data
2. Filter to units with Cycle-to-date Data Usage > 0
3. Join with devices.csv on ICCID
4. Filter to TIG device type only
5. Load vehicles.csv and filter to Not Communicating status
6. Join Not Communicating vehicles to TIG devices on DSN
7. Display both counts:
   - TIG devices with active data usage
   - Not Communicating vehicles that have active cellular data
8. Prompt to continue

## Output
Console output shows:
- Count of TIG devices with active cellular data
- Count of Not Communicating vehicles with active cellular data usage
The second count reveals potential monitoring issues where devices send data but don't report status.

## Example Output
```
============================================================
SHADOW AUDIT: TIG Devices with Active Data Usage
============================================================

TIG devices with Cycle-to-date Data Usage > 0: 170424
Not Communicating vehicles with active cellular data: 12345

Press Enter to continue...
```

## Troubleshooting

**Error: Missing file**
- Ensure both OEM Historical Usage.xlsx and devices.csv are in the shadow-audit/ directory
- Check file names match exactly (case-sensitive on Linux/Mac)

**Error: No module named pandas/openpyxl**
- Run: pip install pandas openpyxl
