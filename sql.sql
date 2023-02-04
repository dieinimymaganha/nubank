CREATE TABLE "country" (
  "country_id" uuid PRIMARY KEY,
  "country" varchar(128)
);

CREATE TABLE "state" (
  "state_id" uuid PRIMARY KEY,
  "state" varchar(128),
  "country_id" uuid
);

CREATE TABLE "city" (
  "city_id" uuid PRIMARY KEY,
  "city" varchar(256),
  "state_id" uuid
);

CREATE TABLE "customers" (
  "customer_id" uuid PRIMARY KEY,
  "first_name" varchar(128),
  "last_name" varchar(128),
  "customer_city" uuid,
  "country_name" varchar(128),
  "cpf" varchar(11)
);

CREATE TABLE "accounts" (
  "account_id" uuid PRIMARY KEY,
  "customer_id" uuid,
  "created_at" timestamp,
  "status" varchar(128),
  "account_branch" varchar(128),
  "account_check_digit" varchar(128),
  "account_number" varchar(128)
);

CREATE TABLE "transfer_ins" (
  "id" uuid PRIMARY KEY,
  "account_id" uuid,
  "amount" float,
  "transaction_requested_at" int,
  "transaction_completed_at" int,
  "status" varchar(128)
);

CREATE TABLE "transfer_outs" (
  "id" uuid PRIMARY KEY,
  "account_id" uuid,
  "amount" float,
  "transaction_requested_at" int,
  "transaction_completed_at" int,
  "status" varchar(128)
);

CREATE TABLE "pix_movements" (
  "id" uuid PRIMARY KEY,
  "account_id" uuid,
  "in_or_out" varchar(128),
  "pix_amount" float,
  "pix_requested_at" int,
  "pix_completed_at" int,
  "status" varchar(128)
);

CREATE TABLE "investments" (
  "transaction_id" uuid PRIMARY KEY,
  "account_id" uuid,
  "type" varchar(128),
  "amount" float,
  "investment_requested_at" int,
  "investment_completed_at" int,
  "status" varchar(128)
);

CREATE TABLE "d_month" (
  "month_id" int PRIMARY KEY,
  "action_month" int
);

CREATE TABLE "d_year" (
  "year_id" int PRIMARY KEY,
  "action_year" int
);

CREATE TABLE "d_time" (
  "time_id" int PRIMARY KEY,
  "action_timestamp" timestamp,
  "week_id" int,
  "month_id" int,
  "year_id" int,
  "weekday_id" int
);

CREATE TABLE "d_week" (
  "week_id" int PRIMARY KEY,
  "action_week" int
);

CREATE TABLE "d_weekday" (
  "weekday_id" int PRIMARY KEY,
  "action_weekday" varchar(128)
);
/*
ALTER TABLE "state" ADD FOREIGN KEY ("country_id") REFERENCES "country" ("country_id");

ALTER TABLE "city" ADD FOREIGN KEY ("state_id") REFERENCES "state" ("state_id");

ALTER TABLE "customers" ADD FOREIGN KEY ("customer_city") REFERENCES "city" ("city_id");

ALTER TABLE "customers" ADD FOREIGN KEY ("customer_id") REFERENCES "accounts" ("customer_id");

ALTER TABLE "transfer_ins" ADD FOREIGN KEY ("account_id") REFERENCES "accounts" ("account_id");

ALTER TABLE "transfer_outs" ADD FOREIGN KEY ("account_id") REFERENCES "accounts" ("account_id");

ALTER TABLE "pix_movements" ADD FOREIGN KEY ("account_id") REFERENCES "accounts" ("account_id");

ALTER TABLE "investments" ADD FOREIGN KEY ("account_id") REFERENCES "accounts" ("account_id");

ALTER TABLE "investments" ADD FOREIGN KEY ("investment_requested_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "investments" ADD FOREIGN KEY ("investment_completed_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "transfer_outs" ADD FOREIGN KEY ("transaction_requested_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "transfer_outs" ADD FOREIGN KEY ("transaction_completed_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "transfer_ins" ADD FOREIGN KEY ("transaction_requested_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "transfer_ins" ADD FOREIGN KEY ("transaction_completed_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "pix_movements" ADD FOREIGN KEY ("pix_requested_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "pix_movements" ADD FOREIGN KEY ("pix_completed_at") REFERENCES "d_time" ("time_id");

ALTER TABLE "d_time" ADD FOREIGN KEY ("week_id") REFERENCES "d_week" ("week_id");

ALTER TABLE "d_time" ADD FOREIGN KEY ("month_id") REFERENCES "d_month" ("month_id");

ALTER TABLE "d_time" ADD FOREIGN KEY ("year_id") REFERENCES "d_year" ("year_id");

ALTER TABLE "d_time" ADD FOREIGN KEY ("weekday_id") REFERENCES "d_weekday" ("weekday_id");*/