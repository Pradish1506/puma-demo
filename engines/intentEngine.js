import { callLLM } from "../ai/llmClient.js";

export async function detectIntent(email) {
   const prompt = `
  You are the **Senior Triage Specialist** for Puma L1 Support Automation.

  **OBJECTIVE**:
  Classify the customer's email into a Single Primary Intent from the list below.
  
  **INTENT CATEGORIES**:
  
  1. **order_status**
     - Questions like "Where is my order?", "Track my package", "Not received yet".
     - Includes status checks for Created, Packed, or Shipped orders.
  
  2. **refund_not_received**
     - Customer returned item but no money yet.
     - "Where is my refund?", "Money not credited".
     - Claiming return was picked up but status shows otherwise.
  
  3. **cancellation_request**
     - Wants to cancel an open order.
     - "Cancel my order", "Ordered by mistake".
  
  4. **address_change_request**
     - Wants to change shipping address or phone number.
  
  5. **return_exchange_request**
     - Wants to return an item or exchange for size/color.
     - "Does not fit", "Don't like it".
  
  6. **invoice_request**
     - Needs GST invoice or bill copy.
  
  7. **report_problem** (High Touch)
     - Wrong item received, Damaged product, Used product.
     - Missing items in package.
     - Payment deducted but order not placed.
  
  8. **general_inquiry**
     - Store timings, promotions, technical app issues (unrelated to active orders).
     
  9. **unknown**
     - Spam, blank, or unintelligible.

  **RULES**:
  - **Multiple Intent Rule**: If a user asks about Refund AND Order Status, prioritize **refund_not_received**.
  - **Ambiguity**: If the email implies a serious issue (mismatch data, wrong product), lean towards **report_problem**.
  - **JSON ONLY**: Do not add markdown or explanation.
  
  **OUTPUT FORMAT**:
  {
    "intent": "order_status",
    "confidence": 0.95,
    "entities": {
        "new_address": "123 Main St, New York" // null if not found
    }
  }

  **EMAIL**:
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
      console.error("Intent Engine Error:", e);
      return { intent: "unknown", confidence: 0.1 };
   }
}
