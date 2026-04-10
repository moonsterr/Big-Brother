def build_prompt():
    return f"""
You are a business analyst.

Analyze this Reddit post and extract structured insights.

RULES:
- Be objective, not emotional
- Only extract real problems
- Normalize wording (make similar problems consistent)
- Output ONLY valid JSON

SCHEMA:
{{
  "is_problem": boolean,
  "problem_summary": string,
  "problem_category": string,
  "sentiment": float (-1 to 1),
  "agreement_signal": float (0 to 1),
  "business_potential": float (0 to 10),
  "urgency": float (0 to 10),
  "advice": boolean
}}

TEXT:

"""
