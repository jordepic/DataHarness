#!/bin/bash
cp README.md docs/readme.md
BUNDLE_PATH=vendor/bundle bundle exec jekyll serve --baseurl ""
