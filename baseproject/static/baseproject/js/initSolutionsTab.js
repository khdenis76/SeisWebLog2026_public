export function initSolutionsTab() {

    const form = document.getElementById("solutionAddForm");

    if (!form) return;

    const tbody = document.getElementById("bp-solutions-tbody");
    const messageDiv = document.getElementById("solutionMessage");

    function getCSRFToken() {
        const tokenInput = document.querySelector(
            '[name=csrfmiddlewaretoken]'
        );

        if (tokenInput) {
            return tokenInput.value;
        }

        return "";
    }

    function showMessage(text, type = "success") {

        if (!messageDiv) return;

        messageDiv.className = `small mb-2 text-${type}`;
        messageDiv.textContent = text;

        setTimeout(() => {
            messageDiv.textContent = "";
        }, 3000);
    }

    async function refreshDeleteButtons() {

        const deleteButtons = document.querySelectorAll(
            ".delete-solution-btn"
        );

        deleteButtons.forEach((btn) => {

            btn.addEventListener("click", async () => {

                const solutionId = btn.dataset.id;
                const deleteUrl = btn.dataset.deleteUrl;

                if (!confirm("Delete selected solution?")) {
                    return;
                }

                try {

                    const formData = new FormData();
                    formData.append("solution_id", solutionId);

                    const response = await fetch(deleteUrl, {
                        method: "POST",
                        headers: {
                            "X-CSRFToken": getCSRFToken(),
                            "X-Requested-With": "XMLHttpRequest",
                        },
                        body: formData,
                    });

                    const data = await response.json();

                    if (!data.ok) {
                        throw new Error(data.error || "Delete failed.");
                    }

                    tbody.innerHTML = data.html;

                    showMessage(
                        "Solution deleted.",
                        "success"
                    );

                    refreshDeleteButtons();

                } catch (err) {

                    showMessage(
                        err.message,
                        "danger"
                    );
                }
            });
        });
    }

    form.addEventListener("submit", async (event) => {

        event.preventDefault();

        const addUrl = form.dataset.addUrl;

        try {

            const formData = new FormData(form);

            const response = await fetch(addUrl, {
                method: "POST",
                headers: {
                    "X-CSRFToken": getCSRFToken(),
                    "X-Requested-With": "XMLHttpRequest",
                },
                body: formData,
            });

            const data = await response.json();

            if (!data.ok) {
                throw new Error(data.error || "Add failed.");
            }

            tbody.innerHTML = data.html;

            form.reset();

            showMessage(
                "Solution added.",
                "success"
            );

            refreshDeleteButtons();

        } catch (err) {

            showMessage(
                err.message,
                "danger"
            );
        }
    });

    refreshDeleteButtons();
}