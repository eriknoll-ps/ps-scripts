Get your auth token from the browser:

Open PACCAR Solutions in Chrome → DevTools (F12)
Go to Application → Local Storage → https://paccarsolutions.com
Find pnet.portal.encodedToken and copy its value (strip the surrounding quotes)


Set the token either by:

Pasting directly into AUTH_TOKEN = "..." in the script, or
Running export PACCAR_AUTH_TOKEN="your_token" before executing


Install dependencies (if needed): pip install requests
Run: python paccar_t521_check.py