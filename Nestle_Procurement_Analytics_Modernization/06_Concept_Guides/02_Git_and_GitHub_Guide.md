# Git & GitHub — Complete Guide with Real-World Analogies

## 1. Git vs. GitHub — they are not the same thing

**Git** is the version control *tool* — it runs on your machine and tracks changes to files over time. It works completely offline and doesn't need GitHub at all.
**GitHub** is a *hosting and collaboration platform* built around git — it stores a copy of your git repository in the cloud and adds things git itself doesn't have: pull requests, code review, issue tracking, and CI/CD (GitHub Actions).

**Analogy:** Git is like the "track changes" and "save version" engine in a document editor — it works even with no internet. GitHub is Google Drive/SharePoint for that document — a shared place everyone syncs to, comments on, and reviews changes in.

## 2. Core concepts, explained with analogies

| Concept | What it is | Analogy |
|---|---|---|
| **Repository (repo)** | A folder git is tracking the full history of. | A project binder that keeps every past draft, not just the current one. |
| **Commit** | A saved snapshot of changes, with a message explaining why. | A labeled photograph of the project at one moment — "added incremental logic to fact_procurement_spend." |
| **Branch** | An independent line of development, starting from some commit. | A parallel timeline where you can experiment without touching the "real" timeline until you're ready. |
| **`HEAD`** | A pointer to whatever commit/branch you currently have checked out. | The bookmark showing "you are here" in the project's history. |
| **Remote** | A version of the repo hosted elsewhere (e.g., on GitHub), referred to as `origin` by convention. | The shared master copy in the cloud that everyone's local copy syncs with. |
| **Staging area (the "index")** | A holding area where you pick exactly which changes will go into the *next* commit. | Packing a box for shipping — you choose what goes in this specific box before sealing it, even if your desk has other unrelated stuff lying around. |
| **Working directory** | The actual files on disk, as you're editing them right now. | Your literal desk — the mess before anything's packed or shipped. |
| **Merge conflict** | Two branches changed the same lines differently, and git can't guess which one you want. | Two people editing the same paragraph of a shared document at the same time — someone has to manually decide the final wording. |

## 3. Everyday commands

```bash
git init                    # start tracking a brand-new folder as a git repo
git clone <url>              # copy an existing remote repo (and its full history) to your machine

git status                   # what's changed, what's staged, what branch you're on
git add <file>                # stage a specific file's changes for the next commit
git add .                    # stage everything changed in the current folder
git commit -m "message"       # save a snapshot of everything currently staged
git diff                     # see exactly what changed, line by line, not yet staged
git diff --staged            # see what's staged, about to be committed
git log --oneline --graph     # a compact, visual history of commits and branches
```

**Analogy for `add` then `commit` being two separate steps:** you don't have to ship your entire desk every time — you pack (`add`) exactly what's ready, then seal and label the box (`commit`), even while unrelated unfinished work sits on the desk untouched.

## 4. Branching & merging

```bash
git branch                       # list local branches
git branch feature/incremental-model   # create a new branch (doesn't switch to it)
git switch feature/incremental-model   # switch to it (modern syntax)
git switch -c feature/incremental-model # create AND switch in one step
git checkout feature/incremental-model  # older syntax, does the same as switch

git merge feature/incremental-model     # (while on main) bring that branch's changes into main
git branch -d feature/incremental-model # delete a branch once it's merged in
```

**Analogy:** you branch off `main` the way you'd duplicate a document before making a risky edit — if it doesn't work out, you just discard the copy; if it works, you merge the good parts back into the original.

## 5. Working with a remote (GitHub)

```bash
git remote -v                 # show configured remotes and their URLs
git fetch origin              # download the latest remote history WITHOUT changing your local files
git pull origin main           # fetch AND merge the remote branch into your current branch
git push origin main           # upload your local commits to the remote
git push -u origin feature/x    # push a new branch and set it to track the remote one going forward
```

**`fetch` vs. `pull` is a common point of confusion:** `fetch` is like checking the group chat for new messages without acting on them yet — you can look before deciding what to do. `pull` is fetch *plus* immediately merging those changes into your current work — like reading the group chat and instantly updating your own notes to match.

## 6. Undoing things (know these before you need them under pressure)

```bash
git restore <file>              # discard uncommitted changes to a file, back to last commit
git restore --staged <file>      # unstage a file (keeps the actual edits, just un-packs the box)
git reset --soft HEAD~1           # undo the last commit, keep changes staged
git reset --mixed HEAD~1          # undo the last commit, keep changes but unstaged (default mode)
git reset --hard HEAD~1           # undo the last commit AND discard the changes entirely — destructive
git revert <commit-hash>          # create a NEW commit that undoes an earlier one (safe on shared branches)
git stash                        # temporarily shelve uncommitted changes to switch tasks
git stash pop                    # bring shelved changes back
```

**Important distinction:** `reset --hard` rewrites history and throws away work — fine on a branch only you have, dangerous on a shared branch others have already pulled. `revert` is the safe equivalent for shared history: instead of erasing the past, it adds a new commit that cancels it out, so everyone's history stays consistent.

**Analogy for `stash`:** you're halfway through rearranging a room when someone urgently needs the space cleared *right now* — `stash` is shoving everything into a drawer so you can deal with the urgent thing, then pulling it back out (`stash pop`) exactly as you left it.

## 7. GitHub-specific concepts

- **Pull Request (PR):** a request to merge one branch into another, opened on GitHub, where teammates review the actual diff, leave comments, and approve (or request changes) before it merges — the collaboration/review layer git itself doesn't provide.
- **Issues:** GitHub's built-in tracker for bugs/tasks/discussion, linkable to commits and PRs.
- **GitHub Actions:** CI/CD — automatically run something (e.g., `dbt build`, a test suite) every time code is pushed or a PR is opened, so problems are caught before merge, not after.
- **README.md:** the landing page shown when anyone opens the repo on GitHub.
- **`.gitignore`:** a list of file patterns git should never track (e.g., `target/` for a dbt project, `.env` files with secrets) — critical for keeping credentials out of a public repo entirely.

## 8. A realistic end-to-end workflow

Using this very project (`Nestle_Procurement_Analytics_Modernization`) as the running example:

```bash
git clone https://github.com/Data-Anatomy-DE/my_Personal_files.git
cd my_Personal_files/Nestle_Procurement_Analytics_Modernization

git switch -c feature/add-supplier-scd2-snapshot   # 1. branch off before touching anything
# ...edit/add a dbt snapshot file for supplier_snapshot...
git status                                        # 2. see what changed
git add snapshots/supplier_snapshot.sql            # 3. stage just that file
git commit -m "Add SCD2 snapshot for supplier_master"    # 4. commit with a clear message
git push -u origin feature/add-supplier-scd2-snapshot  # 5. push the branch to GitHub
# 6. open a Pull Request on GitHub, get it reviewed
# 7. once approved and merged, clean up:
git switch main
git pull origin main
git branch -d feature/add-supplier-scd2-snapshot
```

## 9. Why this actually matters for a data engineer specifically

- **Rollback safety** — a transformation logic bug can be reverted to the exact last-known-good version in seconds, instead of trying to remember what the SQL used to look like.
- **Audit trail** — every change to business logic (e.g., how `fact_procurement_spend` calculates a total) has a commit message, an author, and a timestamp — genuinely useful when someone later asks "why did this number change?"
- **Safe collaboration** — branches let multiple people work on different models at once without stepping on each other, and PR review catches mistakes (a wrong join, a missing test) before they hit a shared/production environment.
- **CI as a safety net** — GitHub Actions can run `dbt build` automatically on every PR, so a broken model is caught by a machine before it's caught by a stakeholder's dashboard looking wrong.
