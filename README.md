## RIPPER [public]
v4

1. rotating header
2. multi-thread
3. multi-target
4. json dump
5. windows
6. file seeker

max_depth=4:

- How deep the crawler goes into links
- Example: Home → Page → Subpage → Deeper page (4 levels)
- Higher number = more pages but slower
- Lower number = faster but might miss content

max_concurrent=150:

- How many URLs it checks at once
- Like opening 150 browser tabs simultaneously
- Higher number = faster but might trigger site blocks
- Lower number = slower but more stealthy

Recommendation:

- For stealth: max_depth=3, max_concurrent=50
- For speed: max_depth=4, max_concurrent=150
- For deep search: max_depth=5, max_concurrent=100
