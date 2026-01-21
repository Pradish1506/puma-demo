import { callLLM } from "../ai/llmClient.js";

export async function detectRisk(email) {
  const prompt = `
  You are the **Risk & Compliance Officer** for Puma Customer Support.
  
  **OBJECTIVE**:
  Analyze the incoming customer email and detect any **HIGH-RISK** triggers that require immediate escalation to a human agent.
  
  **HIGH-RISK KEYWORDS & CONCEPTS**:
  Scan for the following specific themes. Matches can be exact or semantic (intent-based).
  
  1. **Legal Action**: "Lawyer", "Sue", "Legal", "Court", "Case", "Notice", "Consumer Forum".
  2. **Fraud / Scam**: "Fraud", "Scam", "Cheated", "Fake", "Counterfeit".
  3. **Financial Disputes**: "Chargeback", "Dispute", "Bank complaint", "Unauthorized transaction".
  4. **Harassment / Abuse**: Profanity, threats to staff, abusive language.
  5. **Social Media Escalation**: "Twitter", "X.com", "LinkedIn", "Facebook", "Viral", "Post online", "Influencer".
  6. **Government / Police**: "Police", "FIR", "Complaint", "Authorities".
  
  **OUTPUT RULES**:
  - If ANY of the above are detected \u2192 **risk: true**.
  - If the email is a standard complaint (delayed delivery, refund pending) without threats \u2192 **risk: false**.
  - **Reason**: meaningful short extraction of the keyword or phrase found.
  
  **JSON OUTPUT ONLY**:
  {
    "risk": true,
    "reason": "Threatened consumer court"
  }
  
  **EMAIL TO ANALYZE**:
  Subject: ${email.subject}
  Body:
  ${(email.body_preview || "").substring(0, 3000)}
  ${(email.body?.content || "").substring(0, 3000)}
  `;

  try {
    const res = await callLLM(prompt);
    const cleaned = res.trim().replace(/^```json/, "").replace(/```$/, "").trim();
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("Risk Engine Error:", e);
    return { risk: false, reason: "error" };
  }
}
