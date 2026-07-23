# cronstable for iOS (proprietary)

The closed-source native iOS client for cronstable. It is **not** open source and
is **not** covered by the repository's MIT license.

- License: [LICENSE](LICENSE) (all rights reserved).
- Repository licensing policy: [../LICENSING.md](../LICENSING.md).
- Trademarks: [../TRADEMARKS.md](../TRADEMARKS.md).

## Model

- **Distribution:** the Apple App Store, under Apple's standard Licensed
  Application EULA (or a custom EULA).
- **Monetization:** StoreKit subscriptions / in-app purchases for premium tiers.
- **Backend:** the app talks to a cronstable daemon's HTTP control API and/or a
  hosted cronstable backend. Premium entitlements are verified **server-side**
  (an App Store transaction / receipt is the source of truth); the app never
  embeds a secret to "unlock" features locally, because an app binary can be
  inspected.
- **Bundled open-source components:** any MIT / BSD / Apache libraries the app
  ships are listed in an in-app acknowledgements screen, which satisfies their
  attribution requirement. The MIT cronstable core's notice belongs there if any
  of its code is bundled.

## Boundary rules

- The app consumes the MIT core over its public API; it does not fork or vendor
  the core's source.
- Do not copy MIT-licensed source into this directory. Keep the boundary at the
  API / dependency level.

This directory is currently a **scaffold** (license + model). The Xcode project
lands here later, under this same proprietary license.
