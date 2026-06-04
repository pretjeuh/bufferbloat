# Bufferbloat project — Claude instructions

## Releases

Whenever a change is committed and worth releasing, always do the full release flow without being asked:

1. Update `CHANGELOG.md` with the new version entry (Keep a Changelog format)
2. Commit the changelog
3. Tag the commit (`git tag vX.Y.Z`)
4. Push commits and tag (`git push && git push origin vX.Y.Z`)
5. Create a GitHub release with `gh release create` including a short human-readable description

Use semantic versioning: patch (x.y.Z) for small fixes and UX tweaks, minor (x.Y.0) for new features, major (X.0.0) for breaking changes.
