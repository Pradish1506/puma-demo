import { callLLM } from "../ai/llmClient.js";

export async function decideRoute({ intent, confidence, risk }) {
  const prompt = `
  You are the **Routing Manager** for Puma Support.
  
  **OBJECTIVE**:
  Decide whether the case can be handled by **AI (FCR)** or must be assigned to an **AGENT**.
  
  **INPUT DATA**:
  - Intent: "${intent}"
  - Confidence: ${confidence}
  - Risk Flag: ${risk}
  
  **ROUTING MATRIX**:
  
  1. **CRITICAL OVERRIDE (Risk)**
     - If Risk = true \u2192 **ESCALATE** immediately.
     - Status: "escalated", Owner: "senior_support"
  
  2. **LOW CONFIDENCE**
     - If Confidence < 0.70 \u2192 **AGENT**.
     - Status: "open", Owner: "agent"
  
  3. **INTENT-SPECIFIC RULES**:
  
     A. **Order Status**
        - Standard queries (Where is my order?) \u2192 **AI** (We will fetch status and reply).
        - Exception: If user implies "Stuck for many days", "No movement", "Failed delivery multiple times" \u2192 **AGENT**.
  
     B. **Refund Not Received**
        - Standard query (in timeline) \u2192 **AI**.
        - Exception: "Refund failed", "Bank denied", "Months passed" \u2192 **AGENT**.
  
     C. **Cancellation**
        - \u2192 **AI** (We provide self-serve WhatsApp link).
  
     D. **Address Change**
        - \u2192 **AI** (We strictly inform not supported post-shipment).
  
     E. **Report Problem / Payment Issue / Returns**
        - \u2192 **AGENT** (Requires manual validation).
        
     F. **Invoice**
        - \u2192 **AI** (Auto-email trigger).
  
  **OUTPUT DECISIONS**:
  - **status**: "resolved" (if AI handles), "open" (if Agent), "escalated" (if Risk).
  - **owner**: "ai" or "agent" or "senior_support".
  
  **JSON OUTPUT ONLY**:
  {
    "status": "open",
    "owner": "agent"
  }
  `;

  try {
    const res = await callLLM(prompt);
    const cleaned = res.trim().replace(/^```json/, "").replace(/```$/, "").trim();
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("Decision Engine Error:", e);
    // Safe fallback
    if (risk) return { status: "escalated", owner: "senior_support" };
    return { status: "open", owner: "agent" };
  }
}
