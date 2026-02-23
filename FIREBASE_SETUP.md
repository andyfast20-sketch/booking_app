# Firebase Setup (5 minutes)

## 1. Create Firebase Project
1. Go to [Firebase Console](https://console.firebase.google.com)
2. Click **Add project**
3. Name: `pay-as-you-mow` (or anything)
4. Disable Google Analytics (not needed)
5. Click **Create project**

## 2. Enable Google Sign-In
1. In Firebase console, click **Authentication** (left sidebar)
2. Click **Get started**
3. Click **Sign-in method** tab
4. Click **Google** provider
5. Toggle **Enable**
6. Set **Project support email** (your email)
7. Click **Save**

## 3. Add Your Domain
1. Still in **Authentication** → **Settings** tab
2. Scroll to **Authorized domains**
3. Click **Add domain**
4. Add your live domain (e.g., `payasyoumow.org`)
5. `localhost` is already there for testing

## 4. Get Web Config
1. Click the gear icon (⚙️) → **Project settings**
2. Scroll to **Your apps** section
3. Click the **</>** (Web) icon
4. Register app name: `booking-site`
5. **Don't** enable Firebase Hosting
6. Click **Register app**
7. You'll see `firebaseConfig` — copy these 4 values:
   ```javascript
   apiKey: "AIza..."
   authDomain: "pay-as-you-mow.firebaseapp.com"
   projectId: "pay-as-you-mow"
   appId: "1:123..."
   ```

## 5. Paste Config Into Your Site
1. Open `templates/index.html`
2. Find this block (around line 5210):
   ```javascript
   window.FIREBASE_CONFIG = window.FIREBASE_CONFIG || {
       apiKey: '',
       authDomain: '',
       projectId: '',
       appId: ''
   };
   ```
3. Paste your values:
   ```javascript
   window.FIREBASE_CONFIG = window.FIREBASE_CONFIG || {
       apiKey: 'AIzaSyD...',
       authDomain: 'pay-as-you-mow.firebaseapp.com',
       projectId: 'pay-as-you-mow',
       appId: '1:123...'
   };
   ```

## Done!

Now when customers request a quote:
- They fill the form
- Click **Continue with Google**
- Google popup opens → they pick their account
- Email verified ✅ Quote submitted

**No backend needed** for verification - works even when Render sleeps.
