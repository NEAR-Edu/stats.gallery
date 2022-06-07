ALTER TABLE ONLY account_badge
    ADD CONSTRAINT fk__account_badge__account FOREIGN KEY (account_id) REFERENCES account(id);
