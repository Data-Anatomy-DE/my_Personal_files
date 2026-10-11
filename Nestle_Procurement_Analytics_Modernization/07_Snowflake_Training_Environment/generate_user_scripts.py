"""Generate per-trainee Snowflake SQL (users, roles, sandbox DBs) from an email list.

Usage:  python generate_user_scripts.py "<path to User List.txt>" [output_dir]

Output (default ./_generated, git-ignored because it holds temporary passwords and emails):
  02_create_users.sql   run as ACCOUNTADMIN after 01_shared_setup.sql
  credentials.csv       login + temporary password for each trainee
  99_teardown.sql       drops every user, role and sandbox created above
"""
import csv
import re
import secrets
import string
import sys
from pathlib import Path

SCHEMAS = ["RAW", "STAGING", "MART"]  # same layers as the Nestle project
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def read_emails(path):
    emails = []
    for token in re.split(r"[,\s;]+", Path(path).read_text(encoding="utf-8-sig")):
        token = token.strip().lower()
        if not token:
            continue
        if not EMAIL_RE.match(token):
            sys.exit(f"Not a valid email: {token!r}")
        if token not in emails:
            emails.append(token)
    return emails


def temp_password():
    alphabet = string.ascii_letters + string.digits
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(14)) + secrets.choice("!#$%") + "9"
        if any(c.islower() for c in pw) and any(c.isupper() for c in pw) and any(c.isdigit() for c in pw):
            return pw


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    emails = read_emails(sys.argv[1])
    out = Path(sys.argv[2] if len(sys.argv) > 2 else "_generated")
    out.mkdir(parents=True, exist_ok=True)

    users, seen = [], {}
    for email in emails:
        login = email.split("@")[0]
        ident = re.sub(r"[^A-Za-z0-9]+", "_", login).strip("_").upper()
        if ident in seen:
            sys.exit(f"Name collision: {email} and {seen[ident]} both map to {ident}")
        seen[ident] = email
        users.append({"email": email, "login": login, "ident": ident, "password": temp_password()})

    sql = ["-- Generated. Run as ACCOUNTADMIN after 01_shared_setup.sql. Contains temp passwords: do not commit.", ""]
    for u in users:
        i = u["ident"]
        sql += [
            f"-- {u['login']}",
            "USE ROLE USERADMIN;",
            f"CREATE ROLE IF NOT EXISTS TRN_{i};",
            f"GRANT ROLE TRAINEE_BASE TO ROLE TRN_{i};",
            f"GRANT ROLE TRN_{i} TO ROLE SYSADMIN;",
            "USE ROLE SYSADMIN;",
            f"CREATE DATABASE IF NOT EXISTS SBX_{i};",
        ]
        sql += [f"CREATE SCHEMA IF NOT EXISTS SBX_{i}.{s};" for s in SCHEMAS]
        sql += [
            f"DROP SCHEMA IF EXISTS SBX_{i}.PUBLIC;",
            f"GRANT OWNERSHIP ON DATABASE SBX_{i} TO ROLE TRN_{i} COPY CURRENT GRANTS;",
        ]
        sql += [f"GRANT OWNERSHIP ON SCHEMA SBX_{i}.{s} TO ROLE TRN_{i} COPY CURRENT GRANTS;" for s in SCHEMAS]
        sql += [
            "USE ROLE USERADMIN;",
            f"CREATE USER IF NOT EXISTS {i}",
            f"  LOGIN_NAME = '{u['login']}'",
            f"  EMAIL = '{u['email']}'",
            f"  PASSWORD = '{u['password']}'",
            "  MUST_CHANGE_PASSWORD = TRUE",
            f"  DEFAULT_ROLE = TRN_{i}",
            "  DEFAULT_WAREHOUSE = TRAINING_WH",
            f"  DEFAULT_NAMESPACE = SBX_{i}.RAW;",
            f"GRANT ROLE TRN_{i} TO USER {i};",
            "",
        ]
    (out / "02_create_users.sql").write_text("\n".join(sql) + "\n", encoding="utf-8")

    td = ["-- Generated. Removes all trainee users, roles and sandboxes. Irreversible.", ""]
    for u in users:
        i = u["ident"]
        td += [
            "USE ROLE USERADMIN;", f"DROP USER IF EXISTS {i};",
            "USE ROLE SYSADMIN;", f"DROP DATABASE IF EXISTS SBX_{i};",
            "USE ROLE USERADMIN;", f"DROP ROLE IF EXISTS TRN_{i};", "",
        ]
    (out / "99_teardown.sql").write_text("\n".join(td) + "\n", encoding="utf-8")

    with open(out / "credentials.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["email", "snowflake_login", "temporary_password"])
        for u in users:
            w.writerow([u["email"], u["login"], u["password"]])

    print(f"{len(users)} trainees -> {out.resolve()}")


if __name__ == "__main__":
    main()
