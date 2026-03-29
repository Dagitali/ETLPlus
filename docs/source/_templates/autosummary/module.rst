{% set excluded_members = [] %}
{% if fullname == 'etlplus.api' %}
{%   set excluded_members = [
       'CursorPaginationConfigDict',
       'PagePaginationConfigDict',
       'PaginationClient',
       'PaginationConfig',
       'PaginationType',
       'Paginator',
       'RateLimitConfig',
       'RateLimitConfigDict',
       'RateLimiter',
     ] %}
{% endif %}
{{ fullname | escape | underline }}

.. automodule:: {{ fullname }}
   :no-members:

{% if modules %}
   .. rubric:: Modules

   .. autosummary::

{%   for item in modules %}
      {{ item }}
{%   endfor %}
{% endif %}
{% if functions %}
   .. rubric:: Functions

   .. autosummary::

{%   for item in functions %}
      {{ item }}
{%   endfor %}
{% endif %}
{% if classes %}
   .. rubric:: Classes

   .. autosummary::

{%   for item in classes %}
{%     if item not in excluded_members %}
      {{ item }}
{%     endif %}
{%   endfor %}
{% endif %}
{% if exceptions %}
   .. rubric:: Exceptions

   .. autosummary::

{%   for item in exceptions %}
      {{ item }}
{%   endfor %}
{% endif %}
{% if attributes %}
   .. rubric:: Attributes

   .. autosummary::

{%   for item in attributes %}
      {{ item }}
{%   endfor %}
{% endif %}
