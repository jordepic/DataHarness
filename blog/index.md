---
layout: page
title: Blog
permalink: /blog/
---

Read articles and updates about DataHarness development, releases, and insights.

{% if site.posts.size > 0 %}
{% for post in site.posts %}

## [{{ post.title }}]({{ post.url | relative_url }})

_{{ post.date | date: "%B %d, %Y" }}_

{{ post.excerpt }}

{% endfor %}
{% else %}
No blog posts yet. Check back soon!
{% endif %}
