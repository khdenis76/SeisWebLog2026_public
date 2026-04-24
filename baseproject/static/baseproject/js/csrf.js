export function getCSRFToken() {
  const el = document.querySelector('input[name="csrfmiddlewaretoken"]');
  return el ? el.value : "";
}
