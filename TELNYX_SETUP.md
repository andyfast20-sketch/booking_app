# Telnyx SMS verification setup

This app can send customer phone verification codes via SMS.

If Telnyx is configured, it is used first. If Telnyx is not configured, the app falls back to SMSAPI.com (if configured). If neither provider is configured, phone verification is skipped.

## Option A: Configure in the admin dashboard (recommended)

1. Open the admin dashboard.
2. Find **SMS Verification (Telnyx)**.
3. Paste your **Telnyx API Key**.
4. Enter your **From number** (your Telnyx number) in E.164 format (example: `+447123456789`).
5. Click **Save Telnyx settings**.

Notes:
- The API key input can be left blank on future saves to keep the saved key.
- Your Telnyx number must be SMS-capable.

## Option B: Configure with environment variables (deployment-friendly)

Set these environment variables:

- `TELNYX_API_KEY` = your Telnyx API key
- `TELNYX_FROM_NUMBER` = your Telnyx number in E.164 format (example: `+447123456789`)

If env vars are set, they will be used even if the admin panel settings are blank.

## Troubleshooting

- If you see “Telnyx not configured”, make sure BOTH the API key and from number are set.
- If messages fail, check that the Telnyx number supports SMS and that your Telnyx account is allowed to send to the destination country.
