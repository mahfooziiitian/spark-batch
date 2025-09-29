from requests_html import HTMLSession

session = HTMLSession()
r = session.get("https://docs.databricks.com/api/workspace/jobs/getrun")
r.html.render(timeout=20)  # runs JavaScript

html = r.html.html
print(html[:500])
