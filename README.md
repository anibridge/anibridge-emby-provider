# anibridge-emby-provider

An [AniBridge](https://github.com/anibridge/anibridge) provider for [Emby](https://emby.media/).

_This provider comes built-in with AniBridge, so you don't need to install it separately._

## Configuration

```yaml
library_provider_config:
  emby:
    url: ...
    token: ...
    user: ...
    # sections: []
    # genres: []
    # strict: true
```

### `url`

`str` (required)

The base URL of the Emby server (e.g., http://localhost:8096).

### `token`

`str` (required)

The Emby API token. You can generate this under your user settings in the Emby admin dashboard.

### `user`

`str` (required)

The Emby user to synchronize. This can be a user id, username, or display name.

### `sections`

`list[str]` (optional, default: `[]`)

A list of Emby library section names to constrain synchronization to. Leave empty/unset to include all sections.

### `genres`

`list[str]` (optional, default: `[]`)

A list of genres to constrain synchronization to. Leave empty/unset to include all genres.

### `strict`

`bool` (optional, default: `True`)

When enabled, show/season/episode mappings are restricted to the section's highest-priority TV show metadata downloader from Jellyfin library options. For example, if the top TV metadata downloader is AniDB, only AniDB mapping descriptors will be considered for matching. When disabled, all metadata downloaders will be considered for matching. This option is enabled by default.
