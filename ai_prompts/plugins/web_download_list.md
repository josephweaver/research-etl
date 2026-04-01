# `web_download_list`

Use for:

- HTTP/HTTPS downloads from known URLs

Good fit:

- direct file downloads
- one URL or a small list of URLs
- deterministic output filenames via `out_file`

Important notes:

- `out_file` is only valid when exactly one URL is provided
- for opaque download URLs, prefer `out_file` so the saved filename is stable
- use `urls` for inline URLs
- use `urls_file` when the source list already exists as a text file

Common mistakes:

- using `out_file` with multiple URLs
- assuming the URL path will yield a useful filename
- using this plugin for Google Drive, FTP, or STAC sources

Preferred pattern:

```yaml
plugin: web_download_list
args:
  out: "{rawdir}"
  out_file: "{rawdir}/gSSURGO_IA.zip"
  overwrite: false
  timeout_seconds: 900
  urls: "https://example.org/file.zip"
```
