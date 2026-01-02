# Fabric Dataflows - voa-data-warehouse

Connected workspace: `voa-data-warehouse` (4241c9c7-5818-4c56-9220-faf632741770)

## Working with Dataflows

Each folder ending in `.Dataflow` contains:
- `mashup.pq` - Your M code (edit this)
- `queryMetadata.json` - Query metadata
- `.platform` - System file (don't touch)

## Workflow

1. Edit any `mashup.pq` file locally
2. Commit changes: `git add . && git commit -m "your message"`
3. Push: `git push`
4. Go to Fabric workspace > Source control > Update from Git
5. Changes are now live in Fabric

## Quick Commands

```powershell
# See what changed
git status

# Commit changes
git add .
git commit -m "Updated dataflow logic"
git push

# Pull latest from Fabric
git pull
```

That's it. You can now edit dataflows with Copilot locally.
Microsoft Fabric Dataflows source control
