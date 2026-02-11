# Client-side instructions: from DB setup to `SELECT * FROM students`

Make sure you are running from the root of crusty!

## 1. Start the info server

In one terminal, start the server. Logs of what is happening will be dumped here.

```bash
./info-server.sh
```

Leave this terminal running.

---

## 2. Start the client

In a **second** terminal, start the CrustyDB client:

```bash
./client.sh
```

---

## 3. Create and connect to the database

At the `[crustydb]>>` prompt, create a database:

```text
\r testdb
```

Then connect to the database:

```text
\c testdb
```

---

## 4. Create table

Create an empty table with a schema that is suitable for us to import our csv:

```sql
CREATE TABLE test (a INT, b INT, primary key (a));
```

Import data into this table:

```
\i data.csv test
```

---

## 5. Run a simple scan query

```sql
SELECT * FROM students;
```

You should see the result set printed in the client. After the final milestone, you'll also be able to run more complex queries with filters and joins!

---

