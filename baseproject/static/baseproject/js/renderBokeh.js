export function renderBokehInto(divId, jsonItem) {
  if (!jsonItem) return;

  const div = document.getElementById(divId);
  if (!div) {
    console.warn("Bokeh target div not found:", divId);
    return;
  }

  // Clear previous plot
  div.innerHTML = "";

  // Inject target id
  jsonItem.target_id = divId;

  // Render
  Bokeh.embed.embed_item(jsonItem);
}