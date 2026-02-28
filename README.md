# anibridge-emby-provider

Emby library provider implementation for the AniBridge project.

## Configuration

### `url` (`str`)

The base URL of the Emby server (e.g., `http://localhost:8096`).

### `token` (`str`)

The Emby API token.

### `user` (`str`)

The Emby user to synchronize. This can be a user id or username.

### `sections` (`list[str]`, optional)

A list of Emby library section names to constrain synchronization to. Leave empty/unset to include all sections.

### `genres` (`list[str]`, optional)

A list of genres to constrain synchronization to. Leave empty/unset to include all genres.

### `strict` (`bool`, optional)

When enabled mapping matches are restricted to the section's highest-priority metadata fetcher from the Emby library options. For example, if the top TV metadata fetcher is TMDB, only TMDB mapping descriptors are considered for matching. This option is enabled by default.

When disabled, all mapping descriptors are considered for matching in order of priority. This may result in more matches but can lead to less accurate mappings.

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
