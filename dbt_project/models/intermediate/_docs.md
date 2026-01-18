{% docs transaction_dt %}
Date the order has been executed.
{% enddocs %}

{% docs customer_email_txt %}
Email of the portfolio owner.
{% enddocs %}

{% docs company_number_key %}
Company number of the financial institution executing the transaction.
{% enddocs %}

{% docs company_number_system_cd %}
Local system used by the country of domiciliation to identify a company - such as SIRET in France, CRN in the UK.
{% enddocs %}

{% docs wrapper_key %}
Unique key corresponding to the wrapper the transaction has been executed within.
{% enddocs %}

{% docs asset_local_name_txt %}
Name of the asset bought or sold as part of the transaction.
{% enddocs %}

{% docs order_type_cd %}
Nature of the order executed - Buy or Sell.
{% enddocs %}

{% docs order_subtype_cd %}
Adds context on the reason an order has been executed (asset acquisition, fees, or switches). Switches, in particular, can inform on a change in the investment strategy. Note that an operation will be flagged as part of a switch order if reported as such by the investment platform. Some platforms simply send multiple unrelated orders.
{% enddocs %}

{% docs transaction_details_txt %}
Additional details provided by the investment platform about the transaction.
{% enddocs %}

{% docs quantity_held_num %}
Total quantity of assets exchanged during this transaction.
{% enddocs %}

{% docs unit_price_gbp_num %}
Price paid per unit for this transaction. The amount is expressed in GBP at the time of execution.
{% enddocs %}