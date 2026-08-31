# Contributing

This repository prototypes the Fullsend adapter described by
[TEP-0170](https://github.com/tektoncd/community/pull/1263). Discuss substantial
API or lifecycle changes before implementation while the TEP is under review.

All commits must include a [Developer Certificate of Origin](https://developercertificate.org/)
sign-off. Create one with:

```sh
git commit -s
```

Run the local checks before submitting a change:

```sh
make verify
make test
go vet ./...
```
