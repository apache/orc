{% comment %}
Generates the list of known bugs in a given release
Parameters:
  releaseName - the name of the release (eg. 1.2.0)
{% endcomment %}

Known issues:

{% for issue in site.data.releases[releaseName]["known-issues"] %}
- [{{issue[0]}}]({{site.jira}}/{{issue[0]}}) {{issue[1]}}
{% endfor %}
