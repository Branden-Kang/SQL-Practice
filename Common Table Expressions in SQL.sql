WITH users_in_texas AS
  (SELECT user_name,
          user_state,
          user_city
   FROM all_users
   WHERE state = 'Texas')SELECT *
FROM users_in_texas

-- users_in_texas is our CTE

WITH users_in_texas AS
  (SELECT user_name,
          user_state,
          user_city
   FROM all_users
   WHERE state = 'Texas'),users_in_amarillo_texas AS
  (SELECT user_name,
          user_state,
          user_city
   FROM users_in_texas
   WHERE user_city = 'amarillo')SELECT *
FROM users_in_amarillo_texas-- When chaining CTEs, make sure to separate them with a comma, and notice the WITH statement is not necessary for each CTE

user_id = get_user_id();
// fetch our user id from a web apiadd_user_to_group(user_id);
// add the user to a groupnotifyGroupOfChange();
//notify the group of the addition

WITH add_emails AS (
  SELECT
    phone AS add_phone,
    Max(donor_email) AS add_email
  FROM
    campaign_calls
  GROUP BY
    phone
),

cleaned_calls AS (
  SELECT
    phone,
    add_email AS donor_email,
    call_outcome,
    campaign
  FROM
    add_emails
    INNER JOIN campaign_calls ON phone = add_phone
),-- notice we use our first CTE, campaign_calls, in the inner join of this CTE. Remember you can use CTEs as if they were tables

calls_with_donations AS (
  SELECT
    *
  FROM
    cleaned_calls
  WHERE
    call_outcome = 'donation'
    AND campaign = 'Endless Quest'
),

donor_records_with_donations AS (
  SELECT
    *
  FROM
    donation_history
    INNER JOIN donor_accounts ON donor_id = id
),
-- a simple join of our donation_history and donor_accounts tables 

consolidated_donations AS (
  SELECT
    email AS donations_email,
    STRING_AGG(amount, ', ') AS donation_history
  FROM
    donor_records_with_donations
  GROUP BY
    email
)

SELECT
  *
FROM
  calls_with_donations
  INNER JOIN consolidated_donations ON donor_email = donations_email
