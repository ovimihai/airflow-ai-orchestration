# Airflow AI Orchestration — Condensed Course Reference

> Purpose: a dense, detailed knowledge base for AI agents answering questions about this course.
> Scope: AI orchestration concepts + building AI-aware DAGs in Apache Airflow using the Common AI provider (`@task.llm`, `@task.agent`, toolsets, human-in-the-loop operators, evaluation).
> Core rule repeated throughout the course: **Airflow controls the workflow; the AI performs bounded tasks inside it (one task, one responsibility).**

---

## PART 0 — Course Overview

- **Module 1 (Introduction):** the mental model — AI orchestration, the prompt→agent spectrum, Airflow as the control plane, and when to use AI vs deterministic code.
- **Module 2 (Design an AI-aware DAG):** turn a business process into a DAG; XCom contracts; branching on AI output; retry policies. Design-focused, no coding required.
- **Module 3 (Agents & toolsets):** `@task.llm` vs `@task.agent`; SQL, hook, MCP, and agent-skills toolsets.
- **Module 4 (Evaluation & human-in-the-loop):** evaluating AI output; LLM-as-a-judge; human-in-the-loop operators (approval, HITL, branch, entry).
- **Running example across the whole course:** a SaaS company receives customer emails in a shared inbox. Every hour a pipeline collects new emails, classifies intent with AI (cancellation / billing / technical support / other), extracts any requested cancellation date, checks the account status, and routes each case (retention queue / billing / support / manual review). No customer account is changed automatically.
- **Audience:** data/analytics/platform engineers adding AI to pipelines; assumes basic Airflow (DAGs, tasks, dependencies) and general LLM awareness.

---

## PART 1 — Core Concepts (Module 1)

### 1.1 What is AI orchestration
- Running model-powered tasks inside a **controlled, dependency-aware workflow** that decides **when** the AI runs, **what context** it receives, **how** its output is validated, and **what happens next**.
- A chat window is a conversation; a pipeline is a workflow with no human reading each answer. In chat **you are the orchestrator** (bring context, judge answers, retry). In a pipeline the **system** must do all that automatically, every run, unattended.
- **Engineers already think this way:** an AI step is just another DAG task with dependencies, retries, timeouts, logs, and quality checks.
- **"Magic box" trap:** don't expect the model to figure everything out. Give it a small, well-defined job; control what it sees; validate what it returns.

### 1.2 Prompt / Chatbot / Assistant / Workflow / Agent
Classify any AI system with **three questions**:
1. **Control** — who decides the next step: you, a predefined system, or the model?
2. **Autonomy** — how much does the model decide on its own?
3. **Execution boundary** — how far does one run reach: a single call, a conversation, fixed steps, or an open loop?

From least to most autonomy:
- **Prompt** — one instruction in, one output out. No memory/tools/loop. You control everything; boundary = single call. The atom everything is built from.
- **Chatbot** — multi-turn with session memory, but you drive every turn. Control = human; autonomy low; boundary = the conversation.
- **Assistant** — a chatbot that also calls tools / takes actions (coding copilot editing a file, helpdesk bot looking up an order), still reacting to you turn by turn under supervision. Control shared; autonomy moderate; boundary = supervised session.
- **Workflow** — a predefined system decides what's next (not human, not model); steps fixed in advance; runs unattended; AI is one bounded task inside. Control = system/orchestrator; autonomy deliberately low; boundary = predefined steps.
- **Agent** — the model plans, picks tools, inspects results, decides the next move AND when to stop. Control = model; autonomy very high; boundary = open-ended (only guardrails limit it).

Key clarifications:
- **Assistant vs agent:** both use tools; difference is *who initiates*. Assistant acts when you prompt then waits; agent keeps going on its own until it decides it's done.
- **Workflow vs agent:** putting an LLM inside a DAG does NOT make the DAG an agent. Fixed steps + model fills one box = deterministic workflow. In an agent the path is not predefined — the model chooses it (tool A→B→C or B→A→C), unknown in advance.
- **Reality:** in production you mostly build **workflows that call LLMs in a controlled environment**, not free-running agents.

### 1.3 Airflow as the control plane
- The orchestrator decides what runs, in what order, with what config, under what limits, and keeps a record. The **model is NOT the control plane and NOT the DAG** — it does bounded reasoning inside a task (instruction + context in, output out). It must not schedule itself, build the pipeline, choose unrestricted data access, or decide if its own output is safe to publish.
- **Airflow primitives mapped to AI needs:**
  - **DAG** — defines the workflow boundary; prevents the model from *becoming* the workflow.
  - **Task** — a unit of work; where the AI gets its one bounded responsibility. Don't make one task do everything (debugging nightmare).
  - **XComs** — explicit hand-offs between tasks; small structured values (label, confidence, rationale), stored in the metadata DB. Not for large blobs — pass a reference instead.
  - **Retries** — configured at task level (`retries`, `retry_delay`); transient model failures (rate limits, timeouts, provider errors). AI retries cost money and may differ — choose policy by cost/variance/risk.
  - **Durable execution** (Common AI provider) — caches completed model/tool calls so a retry of a multi-step agent task resumes from the last completed step.
  - **Connections** — keep credentials out of prompts and code.
  - **Logs** — the record of execution for debugging.
  - **Human-in-the-loop operators** (Airflow 3.1+) — approval becomes a real workflow step: Airflow pauses, waits for a human, then continues on the approved path.
  - **Waitable DAG run API endpoint** (Airflow 3.1+) — an external app triggers a DAG and waits synchronously for a terminal state + selected results; Airflow still owns the run.

### 1.4 When to use AI vs deterministic code
- AI is **probabilistic**: same prompt can give different outputs.
- **Use AI** when the task needs interpreting messy/ambiguous/language-heavy input AND the result can still be checked by code.
- **Keep deterministic** for known rules, exact computation, system-of-record data, permissions, repeatable transformations.
- **Good AI fits:** classification into a fixed label set; extraction of structured fields from documents (invoices, contracts, emails); summarization for human consumption/triage (never replacing the underlying logs/records).
- **Keep deterministic:** calculations/metrics (revenue, balances, SLAs), joins/filters/transformations, and **validation** (schema/type/allowed-value checks — never a second "does this look valid?" prompt).
- **Split pattern (used most in practice):** workflow prepares context → AI does a bounded interpretation → deterministic code validates & routes → a human gates any critical action. Example (cancellation intent): fetch emails → strip records the model shouldn't see → AI classifies into fixed labels → validate the label → route high-confidence cancels to a retention queue → human controls any actual account change. The model interprets the language; the workflow controls the action.
- **Quick quiz answers:** classify tickets into 5 categories = AI; compute MRR from subscriptions = deterministic; extract renewal dates from contract PDFs = AI (+ deterministic checks); decide if a row passes a not-null test = deterministic.

---

## PART 2 — Designing an AI-Aware DAG (Module 2)

### 2.1 From business process to DAG (four steps)
1. **Find the verbs** in the requirement — they are candidate tasks (collect emails, identify intent, extract date, check account, route case). Verbs are only candidates; some are too broad, some deterministic, some not tasks at all (e.g. "avoid automatic account changes" is a design *constraint*, not a task).
2. **Classify each candidate** as deterministic / AI / system-of-record / constraint:
   - Collect emails → deterministic (email API/hook)
   - Normalize & store raw records → deterministic
   - Identify customer intent → AI (NL in, fixed labels out)
   - Extract requested cancellation date → AI-assisted (messy language like "end of the month"), value checked deterministically
   - Check account status → deterministic (system of record)
   - Validate the AI output → deterministic
   - Route the case → deterministic (branch on validated fields; model classifies, DAG owns the branch set)
3. **Draw the DAG** (boundaries + dependencies): fetch new emails → normalize email records → classify intent (AI, bounded) → validate intent output → fetch customer account status (only after validation, to avoid wasted lookups) → route case (deterministic; combines validated AI output + account status into a finite set of branches).
4. **Describe the shared data** between tasks. normalize→classify passes email ID, sender, subject, body text. classify→validate passes email ID, intent, requested cancellation date, confidence, rationale. Always include a **confidence score and rationale**.

**Common mistakes:** one giant AI task (hides too much — split it); using AI for system-of-record facts (use SQL); hidden branching inside the prompt (keep branching deterministic).

### 2.2 The XCom contract for AI tasks
- Hand-off from AI task → next task is where things break if you return free prose. A human-readable blob ("customer seems to want cancellation... I'll send to retention") is a poor boundary; downstream tasks should consume **structured fields**.
- **Producer** (`classify intent AI`, bounded) vs **consumer** (`validate intent output`, deterministic).
- **Payload fields:** `email_id` (ties to source), `intent` (fixed set), `requested_cancellation_date` (ISO date or null), `confidence` (known range, usually 0–1), `needs_review` (flag), `rationale` (short, human-inspectable, great for debugging).
- **Keep out of XComs:** large/sensitive content — XComs are size-limited (metadata DB). Pass a **reference** (e.g. `body_ref`) for PDFs/large reports.
- **Consumer behavior (validation):** email ID present; intent in allowed labels; confidence in expected range; date null or valid ISO; rationale a non-null string. Once valid, `route case` uses only accepted fields with deterministic rules — it never re-reads the email or asks the model what to do.
- **When the contract isn't met:** either raise an exception (stop + notify) or fail `validate intent output` and run a downstream task that runs only on that failure. Always plan for failure.
- **Five things to write down for an XCom contract:** producer & consumer tasks; required fields + allowed values; which fields are for deterministic logic vs human inspection; what stays out of the XCom; what happens when the payload doesn't match.

### 2.3 Branching on AI output (correctly)
- **Wrong:** ask the model to return the task name/ID as free text and feed that straight into the branch. It can invent a nonexistent task, misspell it, return two routes or a sentence; the DAG stops documenting its own path; task IDs get coupled to prompt text.
- **Fix:** model produces a **label from a fixed set**; **deterministic code** maps the label → an existing task ID. Mapping lives in code you own and test, not in the prompt. (e.g. cancellation → `send to retention queue`.)
- **Branch logic:** input is the validated payload; if `needs_review` true → `send to manual review`; else return the task ID matching the intent. Use `.get()` and fall back to manual review on misspelled/unexpected values. Always keep a safety fallback.
- **LLMBranchOperator (Common AI provider):** discovers downstream task IDs automatically from the DAG and **constrains** the model to choose only from that set — can't pick a nonexistent task. Less code, very reliable.
- **Four rules:** branch on validated output (not raw text); routes = a fixed set the DAG owns; the set must be exhaustive (every input, including non-fitting ones, lands on a real branch — usually manual-review fallback); keep the mapping deterministic.

### 2.4 Retry policy for AI tasks
- Airflow retries handle failures (dropped DB connection, 503, file not ready). AI adds complexity: a retry may spend more tokens, take different time, or return a different answer — it's a decision about **cost, variance, and risk**.
- **Four factors:** (1) cost per call; (2) output variance; (3) provider limits (concurrency, mass-retry storms → use pools, backoff, max retry delay); (4) downstream impact (manual-review reroute is cheap; a wrong customer-facing message is not).
- **Good retry candidates:** technical errors (provider/network/worker), invalid JSON (retry at least once). **Don't retry:** low-confidence answers (first answer may be best; don't retry until you send something wrong), missing required input, business/validation failures (fix the code).
- **Example policy for `classify intent AI`:** `retries=1`; `execution_timeout=3 min`; `retry_delay=1 min`; `retry_exponential_backoff=true`; `max_retry_delay=10 min`; assign to an **AI provider pool** (slots limit concurrency) to prevent retry storms and control cost.
- **Idempotency:** running a task twice for the same input yields the same output (deterministic = no duplicate writes; AI = no two different decisions affecting downstream). Single-call pattern: key the result by stable input identity + prompt version — idempotency key = `model_id + prompt_version + model_name + normalized_input_hash`; reuse the stored result on retry.
- **Durable execution (multi-step/agent tasks):** a task with a reasoning loop (model→tool→model...). Without it, failure on step 5 restarts from 0. With `durable=true`, completed responses/tool results are cached and replayed, resuming from the failure point. Needs a compatible operator (agent operator) + a configured cache path (local or e.g. a bucket). Cache survives failed attempts within the DAG; cleaned up once the task instance succeeds.

---

## PART 3 — Agents & Toolsets (Module 3)

### 3.1 @task.llm vs @task.agent (do you need an agent?)
- **Single-turn `@task.llm`:** one prompt in, one answer out. Great when everything the model needs is already in front of it (in the prompt or from an upstream task). Prefetch data deterministically and feed it in — cheaper, simpler, faster.
- **Most of the time you do NOT need an agent.** The LLM decorator covers a huge amount of real work.
- **Two signals you DO need an agent:** (1) the model must decide what to look up and you can't script it in advance; (2) the work is a multi-step loop (look → think → act → look again).
- **Code difference:** swap `@task.llm` for `@task.agent`; the key addition is **`toolsets`**. At run time the agent loops model→tool→model→tool until done, all inside one Airflow task.
- **Agent decorator parameters:**
  - `toolsets` — the list of things the agent may do (needs tools to be useful).
  - `enable_tool_logging` — one log line per tool call (which tool, args, result). Almost always ON for agents, else the loop is a black box.
  - `output_type` — defines output shape (Pydantic) for downstream.
  - `agent_params` — fine-tuning (model settings, retries of bad responses).
  - `durable` — cache completed steps; on retry resume near where it left off (set true for multi-step agents).
- **Traps:** using an agent when prefetch + `@task.llm` would do; tool logging off; letting the agent return free text (give it `output_type`); a long/expensive agent with no `durable`/retries.

### 3.2 Toolsets (what they are + SQL toolset)
- **Problem:** a raw DB connection lets a bad reasoning step read too many tables or drop tables. A **toolset** precisely defines what the agent can do.
- **Tool** = one named thing (name + when-to-use description + defined arguments + does one thing). **Toolset** = a bundle of related tools. Passing a toolset = "here is the exact named list of things you're allowed to do."
- **Provider toolsets:** SQL toolset (database tools), hook toolset (hook methods → tools), MCP toolset (tools from an MCP server).
- **SQL toolset:** instantiate `SQLToolset` from `toolsets.sql`; params: `db_connection_id` (e.g. `warehouse_postgres`), allowed tables (e.g. orders, customers), `max_rows`. Exposes four tools: `list_tables`, `get_schema`, `query`, `check_query` (validate SQL syntax without running). Has a read-only/write flag (defaults to no-write).
- **Usage:** `@task.agent(..., toolsets=[SQLToolset(...)], enable_tool_logging=True)`; logs show tool calls in order (list_tables → get_schema → query).

### 3.3 Hook toolset
- **Goal:** turn a trusted Airflow hook into agent tools (e.g. let the agent list files / check timestamps on an SFTP server). A hook abstracts a service for an operator; Airflow has many (S3, SFTP, HTTP, ...).
- **Params:** the hook (e.g. `SFTPHook`), its connection (e.g. `ssh_conn_id` → `partner_sftp`), **`allowed_methods`** (required — no "expose everything"/auto-discovery), optional `tool_name_prefix` (e.g. `sftp_`).
- **CRITICAL — no read-only mode** (unlike SQL toolset). It doesn't know which methods read vs destroy; it builds a tool per name you give. Putting `delete_directory` in `allowed_methods` gives the agent a working delete tool with no second guard. Choosing safe methods is YOUR responsibility.
  - SFTP safe (read): `list_directory`, `path_exists`, `get_mod_time`, `describe_directory`. Dangerous (write/action): `store_file`, `create_directory`, `delete_file`, `delete_directory`.
- **HTTP hook special case:** really one useful method, `run`, which may read or write. Control it in the hook init via the `method` param (default POST; set `GET` to be sure it won't write). Still set connection, `allowed_methods`, prefix.
- **Lesson:** sometimes you control read/write via `allowed_methods`, sometimes in the hook's init (HTTP `method`).
- **Documentation matters:** the agent picks a tool by name/description/parameters; poorly documented or lookalike methods make it choose wrong.

### 3.4 MCP toolset
- **MCP = Model Context Protocol** — a standard way for programs to expose tools to AI models. Use when the tool already exists as a server (weather service, code sandbox) and you don't want to rebuild it.
- Contrast: the hook toolset adapts a specific hook (bespoke, per-system wiring). MCP flips it — the system exposes tools once as an **MCP server** speaking the standard protocol, and any MCP-aware client (agent, chat app, IDE) can connect, discover tools, and learn how to call them. One protocol, many clients.
- **Params:** `mcp_connection_id` (points to an Airflow connection of type **mcp** holding the server URL + credentials — kept out of DAG code), `tool_prefix` (namespaces tool names; important because two MCP servers can expose the same tool name, e.g. `get_forecast` — prefix like `weather_` avoids collisions).
- **Key difference: you do NOT define the tools.** The agent asks the server what tools it has and gets all of them (names, descriptions, args). So you must **trust the MCP server** and read its docs.
- **SQL toolset vs MCP (e.g. Snowflake):** SQL toolset when you want to query safely and control the boundary yourself (you pick the tables); MCP toolset when you want the vendor's **richer tools** beyond plain SQL.

### 3.5 Agent skills toolset
- Other toolsets answer "what can the agent **touch**?"; the agent skills toolset answers "what does the agent **know how to do**?" — it's about **skills/know-how** (how to write the standard sales report, apply a refund policy).
- **A skill** = a folder with a markdown file (`skill.md`). File has a **name + description** at the top (the description tells the model *when* to load it) and a **plain markdown body** (steps/rules).
- **Clever part:** the agent does NOT get every skill's full text in its prompt. It first sees only names + descriptions, then decides relevance and **loads on demand**.
- **Usage:** pass the agent skills toolset to the agent + point at skill sources (e.g. a local directory). Requires installing the Common AI provider with the optional **`skills`** extra.
- **Pair with a capability toolset:** the skills toolset gives **know-how only**, not access. Combine with SQL/hook/MCP (e.g. skills = reporting rules, SQL toolset = the tables). It is NOT a replacement for the others.
- **Skills from Git (real world):** use `GitSkills` — repo, `connection_id` (Airflow connection of type **git**, token stored there), optional path/subfolder, optional branch. Repo is cloned **at runtime on the worker** (not at DAG parse).
- **Security (important):** a skill bundle can include **scripts the agent runs on your worker**, fetched at runtime from a repo anyone with write access could change. Safeguards: (1) trust the repo URL; (2) pin the branch to a reviewed ref (don't default to `main`); (3) if no scripts needed, turn script execution off via `exclude_tools=["run_skill_script"]`; (4) keep credentials in the Airflow git connection and run workers without ambient git credentials for strict isolation.
- **When to use:** reusable procedures the agent should follow the same way every time, kept in one versioned place (Git) instead of pasted into every system prompt.

---

## PART 4 — Evaluation & Human-in-the-Loop (Module 4)

### 4.1 Evaluation (concept)
- An AI task's output flows downstream — always ask **how good is it?** Never assume it's good (impacts customers/production).
- **Evaluation = a check, like a data-quality check.** A normal transform gets a quality check (row count > 0, no nulls) and fails the task if it fails. An AI step gets the same treatment.
- What changes with AI is what "bad" looks like: wrong category, made-up figure, wrong tone, malformed result. Some are catchable with plain asserts (valid JSON? label in the set?), but the ones that matter most have no simple assert (no `assert summary is accurate`) — you need a **verdict** you can act on.
- **Two judges:** LLM-as-a-judge vs human.
- **LLM-as-a-judge:** cheap, fast, scales; but can be confidently wrong and favors outputs that *sound* good. Great first filter, not for subjective/high-stakes alone.
- **Human:** authority & accountability, handles ambiguity; but costly, slow, doesn't scale.
- **Five factors to choose:** (1) cost & scale; (2) reversibility (undoable mistake → AI judge ok; irreversible like sent email / issued refund → human); (3) impact (low internal → AI; customer-facing/money → human); (4) ambiguity (crisp/checkable → AI; nuanced → human).
- **Combined pattern (most common):** LLM judge on everything cheaply; escalate low-confidence/high-impact to a human. Nothing skips evaluation entirely.
- **Traps:** no evaluation step; letting the LLM judge make the final call on high-stakes (it hallucinates); putting a human on everything.

### 4.2 LLM-as-a-judge (implementation)
- Built with the LLM decorator. **Naive version** returns plain text ("looks good") — ambiguous; avoid.
- **Improvement 1 — structured output:** add `output_type` with a Pydantic `Verdict` model: booleans (`pass`, `accurate`, `complete`, ...) + a `reason` string. Turns the judge "from a thing that talks into a thing that decides."
- **Improvement 2 — checklist in the system prompt:** spell out what to check and how strict (e.g. "check accurate, complete, on topic; set pass=true only if all three hold; always give a one-line reason"). Split: standing instructions in the **system prompt**, the changing content (the summary) in the **prompt**.
- **Improvement 3 — give the judge the source:** to check accuracy (not just fluency), pass both the **source** and the output being checked (e.g. `source_text` + `summary`). Rule: whenever you evaluate accuracy, also pass the source.
- **Pipeline:** `fetch document` (source) → `summarize` (AI task evaluated) → `llm as a judge` (also receives source) → verdict. Downstream: if `pass` is false → raise AirflowException (stop); else continue.

### 4.3 Human-in-the-loop operators
- When a HITL operator runs, the task (and DAG) **pauses** until a person responds in the Airflow UI, then continues. Available Airflow 3.1+.
- **Approval operator (simplest):** two options, approve/reject; put it before the action to protect. Approve → downstream runs; reject → downstream skipped. Set `subject` + `body`. Default: a reject does NOT fail the task (the reviewer answered; "no" is valid). Optional `fail_on_reject=true` exists but is discouraged (a reject isn't really a failure).
- **HITL operator (more than two choices):** pass `subject`, `body` (often from an upstream AI task), a **list of options** (e.g. send as is / send with edits / discard) + a **default value**. Result read downstream via `review.output`: `chosen_options` (what they picked), `params_input` (what they typed), plus who responded & when (auditing).
  - Extra params: `multiple=true` (pick more than one option); `assigned_users` (limit who can respond).
  - **Timeouts (important):** always set `response_timeout` (e.g. 4h) or it waits forever. Behavior on timeout depends on the default value: with a default → takes the default option; with no default → the timeout FAILS the task.
- **Common mistakes:** no `response_timeout` (it acts like a sensor, wasting resources); timeout without a default value; gating everything (sometimes an LLM judge is enough; reserve humans for critical/high-stakes).

### 4.4 HITL branch & entry operators (give feedback to your agent)
- **HITL branch operator:** human picks among **different paths** (vs approval's run/skip); the DAG runs the task the option points to and skips the rest (it's a branch task). Define a default value + `response_timeout`.
  - **`options_mapping`:** translation layer, option label (left) → task ID (right), so the reviewer sees plain language (publish / send back for revision) not internal IDs (`collect_feedback`). Rules: every key must be one of the options (else rejected at parse time); every value must be a real downstream task ID. If option labels already equal task IDs, you can skip the mapping.
- **HITL entry operator:** a branch tells you *which* path but not *why*. The entry operator shows a **form** (fields with name/type/default via `params`) to collect the reviewer's actual input; results come back in **`params_input`** (e.g. `params_input["feedback"]`), readable downstream. This is how you pass feedback back to the model.
- **Improvement loop:** branch operator as the review task; if a summary isn't good enough, pick "regenerate" → routes to the entry operator → collect feedback → next generation is better because the model now has feedback. A self-improving loop.
- **Key takeaways:** always define `options_mapping` when you don't want to expose task IDs; always read the entry operator's result via `params_input`.

---

## PART 5 — Single-Call LLM Mechanics (Module 2/3 detail: @task.llm)

### 5.1 A single LLM call
- Use `@task.llm` from the Common AI provider: it turns a Python function into an LLM-backed task. The **function builds the prompt** (it does NOT call the model directly).
- **Step 1 — prepare input:** don't pass raw source (full thread, headers, attachments). Clean/structure it (email ID, sender, subject, body text; `.strip()` the body). Structured input → better answers.
- **Step 2 — write the task:** key args: `llm_connection_id` (which provider connection; token in the connection's password field), `system_prompt` (how the model behaves / task definition, e.g. "classify, return compact JSON, use this label set, do not choose a task or queue"), a function parameter from the upstream task, and the function body returns the prompt string. With no `output_type` it returns a **string** (not recommended — prefer a Pydantic `output_type`).
- **Step 3 — wire into the DAG:** fetch new email → normalize email record → classify intent AI → validate intent output.
- **Mistakes:** passing too much input; provider details/secrets in the prompt (use a connection); reaching for an agent by default; treating model text as validated data (it's a string until you use `output_type` or parse it).

### 5.2 Choosing a model
- Don't default to biggest/smallest/cheapest. **Step 1 — gather task facts** (daily volume, avg/large input tokens, output size, latency target, retry policy, provider limits, quality requirement). Each fact drives the choice (volume → cost sensitivity; input size → capability; latency → backlog risk; retries → worst-case calls).
- **Step 2 — account for attempts:** 20k emails/day + 1 retry = up to ~40k worst case. Estimate tokens/attempt (input+output), daily token volume (records × tokens/attempt), retry-adjusted volume → cost.
- **Step 3 — pick a tier (examples, not permanent):** fast/low-cost (Claude Haiku 4.5, GPT-5 Nano) for short bounded tasks; balanced (Claude Sonnet 4.6, GPT-5 Mini) for ambiguity/longer context; high-capability/long-context (GPT-5.2, Opus 4.8) for large input/harder reasoning. For the email classifier → fast/low-cost.
- **Step 4 — specify the model:** in the Airflow connection's `extra` field, or in code via `model_id` on the decorator. Goal: **visibility** of which model is used.
- **Mistakes:** biggest by default (wastes money); cheapest by default (breaks downstream review); sizing only for average input; ignoring retries/provider limits.

### 5.3 System prompt vs prompt
- **System prompt** = stable every run (task goal, allowed labels, output routes = how the model behaves). **Prompt** = what the function returns, changes per run (depends on inputs = what to do this instance). Whatever the decorator returns IS the prompt.
- **Don't mix them**; harder to review/debug. **Version both** (prompt version = name + version, metadata for logs/tests, maintained by hand). Keep the system prompt in its own importable file.
- Build the runtime prompt in a function (e.g. `build_email_runtime_prompt`) using `json.dumps` to encode special chars. Wire both to the decorator.
- **Attach the prompt version to the output** (e.g. `tag_with_prompt_version` → `{email_id, prompt_version, raw_response}`) for debugging in production.
- **Responsibility:** bump the prompt version whenever you change labels/guardrails/rules.

### 5.4 Model output (output_type)
- By default the decorator returns a **string**; even JSON-looking text isn't validated. **Better:** pass a Pydantic model to `output_type` — if the model's output doesn't match, the task fails immediately (no downstream check needed).
- **Pydantic model example (`SupportIntentOutput`):** required `email_id`, `intent`, `confidence`, `rationale`; optional `requested_cancellation_date` (`| None`); `intent` as a **`Literal`** of allowed values (include "other" for uncertainty); `confidence` constrained 0–1 via `Field`.
- With the model defined, remove output descriptions from the prompt (they live in the Pydantic model). The provider returns a Pydantic instance → serialized → downstream gets a **dict**.
- **Mistakes:** using a string for labels (use `Literal` + an "other" fallback); assuming structure = correctness (still validate the *values*: chosen label, confidence threshold).

### 5.5 Model settings (agent_params)
- `agent_params` is a dict handed to the underlying Agent. Two common keys: **`model_settings`** and **retry options**.
- **model_settings:** `temperature` (0 or near for classification — deterministic labels; high temp is for drafting), `max_tokens` (keep tight for small structured output → cost/latency), `timeout` (per single model call, e.g. 30s).
- **Two kinds of retries (use both):**
  - **Agent retries** via `output_retries` (e.g. 2): model answered but output doesn't fit the Pydantic schema → agent re-asks the model, inside the same task run, no delay, invisible in the UI. Handles a **bad answer**.
  - **Airflow retries** via `retries` (e.g. 2): provider 500 / dropped connection / rate limit → task fails, Airflow waits (e.g. 30s) and retries. Handles a **broken call**.
- **Two timeouts:** the `model_settings` timeout (one call, 30s) must be **smaller** than the task-level `execution_timeout` (whole task, e.g. 2 min).
- **Mistakes:** high temperature for classification; model-call timeout longer than task timeout; mixing up the two retries (bad output → `output_retries`; failing API → Airflow `retries`); cranking agent retries to mask a broken prompt (fix the prompt/output type instead).

---

## PART 6 — Quick Reference / Glossary

- **Core rule:** Airflow controls the workflow; the AI performs bounded tasks inside it (one task, one responsibility).
- **Classification questions:** control / autonomy / execution boundary.
- **`@task.llm`** — single-turn LLM task; function builds the prompt; prefer `output_type`.
- **`@task.agent`** — agentic task with a tool loop; key params `toolsets`, `enable_tool_logging`, `output_type`, `agent_params`, `durable`.
- **Toolsets:** SQL (list_tables/get_schema/query/check_query; has read-only flag), Hook (methods→tools, `allowed_methods` required, NO read-only guard), MCP (connect to an MCP server; tools defined by the server, not you; prefix to avoid collisions), Agent Skills (know-how only; markdown skills loaded on demand; pair with a capability toolset; Git-sourced + script-execution security).
- **XCom contract:** small structured payload (email_id, intent (fixed set), date|null, confidence 0–1, needs_review, rationale); references for large content; validate deterministically; plan for contract failure.
- **Branching:** model returns a label; deterministic code (or LLMBranchOperator) maps label→task ID; routes are a fixed, exhaustive set the DAG owns; always a manual-review fallback.
- **Retries:** choose by cost/variance/provider-limits/downstream-impact; use pools + exponential backoff + max_retry_delay; idempotency key = model_id+prompt_version+model_name+normalized_input_hash; durable execution resumes multi-step tasks.
- **Evaluation:** LLM-as-a-judge (cheap/scales, can be confidently wrong) + human (authority, doesn't scale); choose by cost/scale, reversibility, impact, ambiguity; combine (LLM first, escalate to human).
- **HITL operators:** Approval (approve/reject), HITL (N options, `multiple`, `assigned_users`, `response_timeout` + default), Branch (`options_mapping`), Entry (form → `params_input` feedback).
- **Airflow 3.1+ features:** human-in-the-loop operators, waitable DAG run API endpoint.
- **Common AI provider features:** `@task.llm`, `@task.agent`, toolsets (SQL/hook/MCP/skills), LLMBranchOperator, durable execution.
- **Model tiers (examples, change often):** fast/low-cost (Claude Haiku 4.5, GPT-5 Nano); balanced (Claude Sonnet 4.6, GPT-5 Mini); high-capability (GPT-5.2, Opus 4.8).
