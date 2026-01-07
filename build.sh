#!/bin/bash

cat > docs/index.md << 'EOF'
---
layout: page
title: Documentation
permalink: /docs/
---

EOF

sed 's|!\[\(.*\)\](\(read_path\.png\))|![\1]({{ site.baseurl }}/\2)|g; s|!\[\(.*\)\](\(write_path\.png\))|![\1]({{ site.baseurl }}/\2)|g' readme.md >> docs/index.md

BUNDLE_PATH=vendor/bundle bundle exec jekyll serve --baseurl ""
