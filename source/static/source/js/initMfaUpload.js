export function initMfaUpload() {
    const form = document.getElementById("mfa-upload-form");
    if (!form) return;

    const input = document.getElementById("mfa-files-input");
    const status = document.getElementById("mfa-upload-status");
    const submitBtn = document.getElementById("mfa-upload-submit");
    const progressWrap = document.getElementById("mfa-upload-progress-wrap");
    const progress = document.getElementById("mfa-upload-progress");

    function getCsrfToken() {
        const csrfInput = form.querySelector("input[name='csrfmiddlewaretoken']");
        return csrfInput ? csrfInput.value : "";
    }

    form.addEventListener("submit", async function (event) {
        event.preventDefault();

        const files = input.files;

        if (!files || files.length === 0) {
            status.innerHTML = `<span class="text-danger">Please select at least one MFA file.</span>`;
            return;
        }

        const formData = new FormData();

        for (let i = 0; i < files.length; i++) {
            formData.append("mfa_files", files[i]);
        }

        submitBtn.disabled = true;
        status.textContent = `Uploading ${files.length} MFA file(s)...`;

        progressWrap.classList.remove("d-none");
        progress.style.width = "20%";
        progress.textContent = "Uploading...";

        try {
            const response = await fetch(form.dataset.uploadUrl, {
                method: "POST",
                body: formData,
                headers: {
                    "X-CSRFToken": getCsrfToken(),
                    "X-Requested-With": "XMLHttpRequest",
                },
            });

            const data = await response.json();

            progress.style.width = "100%";
            progress.textContent = "Done";

            if (!response.ok || !data.ok) {
                const failed = data.failed || [];
                const failedHtml = failed.map(item => {
                    return `<div>${item.file_name}: ${item.error}</div>`;
                }).join("");

                status.innerHTML = `
                    <div class="text-danger">
                        MFA upload failed.
                    </div>
                    ${failedHtml}
                `;

                submitBtn.disabled = false;
                return;
            }

            status.innerHTML = `
                <span class="text-success">
                    Imported ${data.imported_count} MFA file(s). Refreshing page...
                </span>
            `;

            window.location.reload();

        } catch (error) {
            status.innerHTML = `<span class="text-danger">${error}</span>`;
            submitBtn.disabled = false;
            progressWrap.classList.add("d-none");
        }
    });
}