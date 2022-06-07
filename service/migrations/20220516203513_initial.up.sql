CREATE TABLE account (
    id text NOT NULL,
    balance numeric(45,0),
    score integer
);

CREATE TABLE account_badge (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id text NOT NULL,
    badge_id uuid NOT NULL
);


ALTER TABLE ONLY account_badge
    ADD CONSTRAINT pk__account_badge PRIMARY KEY (id);

ALTER TABLE ONLY account
    ADD CONSTRAINT pk__account PRIMARY KEY (id);

ALTER TABLE ONLY account_badge
    ADD CONSTRAINT ux__account_badge__account_id__badge_id UNIQUE (account_id, badge_id);

CREATE INDEX ix__account__balance ON account USING btree (balance DESC NULLS LAST);

CREATE INDEX ix__account__score ON account USING btree (score DESC NULLS LAST);

CREATE INDEX ix__account_badge__badge_id ON account_badge USING btree (badge_id);

ALTER TABLE ONLY account_badge
    ADD CONSTRAINT fk__account_badge__account FOREIGN KEY (account_id) REFERENCES account(id);

ALTER TABLE IF EXISTS account
  ADD COLUMN modified timestamp without time zone DEFAULT current_timestamp;

-- https://stackoverflow.com/a/26284695
create or replace function update_modified_column()
returns trigger as $$
begin
  new.modified = now();
  return new;
end;
$$ language 'plpgsql';

create trigger update_account_modified
  before update on account
  for each row
  execute procedure update_modified_column();
