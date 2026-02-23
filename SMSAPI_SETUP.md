# SMSAPI.com Setup Guide

Your booking app now uses **SMSAPI.com** for SMS verification (cheap, simple, zero compliance).

## Cost
- **~£0.02-0.03 per UK SMS** (2-3 pence)
- Free trial credits included when you sign up

## How to Set Up

### 1. Create Account
1. Go to https://www.smsapi.com/
2. Click "Sign Up" or "Get Started"
3. Enter your email and create password
4. **No business verification needed** - instant access

### 2. Get Your OAuth Token
1. Log into your SMSAPI dashboard
2. Go to **Settings → API Password**
3. Click **"Generate OAuth Token"**
4. Copy the token (long random string)

### 3. Configure in Your App
1. Go to your admin panel: https://payasyoumow.org/admin
2. Scroll to **"SMS Verification (SMSAPI.com)"** section
3. Click **Expand**
4. Paste your **OAuth Token**
5. (Optional) Enter **Sender Name** like "PayAsYouMow" (11 chars max)
6. Click **Save SMS settings**

### 4. Test It
1. Go to your homepage
2. Fill out a quote request
3. Enter your mobile number with country code: `+447123456789`
4. You should receive a 4-digit verification code via SMS

## Troubleshooting

**No SMS received?**
- Check your OAuth token is correct
- Make sure phone number includes `+44` for UK
- Check SMSAPI dashboard for credit balance
- Check SMS delivery status in SMSAPI logs

**"OAuth token invalid" error?**
- Regenerate token in SMSAPI dashboard
- Copy the full token (no spaces)
- Save again in admin panel

## Support
- SMSAPI support: https://www.smsapi.com/contact
- SMS logs: Login → Reports → SMS History
