export async function audit(entity, id, action, by = "system") {
  // Just log instead of writing to a database
  console.log(`📝 Audit → Entity: ${entity}, ID: ${id}, Action: ${action}, By: ${by}`);
}
