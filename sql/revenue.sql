SELECT t.ticketID
, t.customerID
, s.subscriptionid
, t.officeID
, t.invoicedate
, DATE_TRUNC('MONTH', t.invoicedate) AS month_start
, ADD_MONTHS(DATE_TRUNC('MONTH', t.invoicedate), 1) - INTERVAL '1 DAY' AS month_end
, CASE WHEN t.servicecharge=0 AND (t.total-t.taxamount) > 0 THEN (t.total-t.taxamount) ELSE t.servicecharge END as svccharge
, t.taxamount
, t.total
, t.isactive
, st.description AS service_type
, COALESCE(SUM(case when p.paymentmethod = 0 then ap.taxcollected end)*(-1),0) as tax_couponed
, (COALESCE(SUM(case when p.paymentmethod = 0 then ap.appliedamount end)*(-1),0)) - tax_couponed as invoice_couponed
, COALESCE(SUM(case when p.paymentmethod > 0 then ap.taxcollected end),0) as tax_revenue
, (COALESCE(SUM(case when p.paymentmethod > 0 then ap.appliedamount end),0)-tax_revenue) as service_revenue
, (svccharge + invoice_couponed) as service_invoiced
, (t.taxamount + tax_couponed) as tax_invoiced
, (service_invoiced - service_revenue) as service_balance
, (tax_invoiced - tax_revenue) as tax_balance

FROM STAGING_DB.FIELDROUTES.FACT_TICKET t
left join staging_db.fieldroutes.fact_appointment a
    on t.ticketID = a.ticketID
left join staging_db.fieldroutes.fact_subscription s
    on a.subscriptionid = s.subscriptionid
inner join staging_db.fieldroutes.dim_service_type st
    on t.serviceID = st.serviceTypeID
left join staging_db.fieldroutes.fact_applied_payment ap
    on t.ticketID = ap.ticketID
inner join staging_db.fieldroutes.fact_payment p
    on ap.paymentid = p.paymentid

where t.isactive=1

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;