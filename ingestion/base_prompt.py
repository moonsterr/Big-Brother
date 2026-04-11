def build_prompt():
    return f"""
You are a business analyst.

Analyze this Reddit post and extract structured insights.

RULES:
 - Identify if the following text describes a business or technical problem. 
 - Output a SINGLE JSON object. 
 - DO NOT include markdown code blocks. 
 - DO NOT include commentary, notes, or trailing text.
 - If no comments exist, agreement_signal MUST be 0.
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
