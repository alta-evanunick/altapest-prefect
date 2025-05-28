SELECT o.office_id,
       v.entity_name
FROM   Ref.offices_lookup o
CROSS JOIN VALUES
  ('customer'),('employee'),('office'),('region'),('serviceType'),('customerSource'),
  ('genericFlag'),('appointment'),('subscription'),('route'),('ticket'),('ticketItem'),
  ('payment'),('appliedPayment'),('note'),('task'),('appointmentReminder'),('door'),
  ('disbursement'),('chargeback'),('flagAssignment') v(entity_name);