export function initExportNodePositionReports() {
    const btn = document.getElementById("btn-export-node-position-reports");

    if (!btn) return;

    btn.addEventListener("click", async function () {

        const selectedLines = Array.from(
            document.querySelectorAll(".dsr-line-checkbox:checked")
        )
        .map(cb => cb.value)
        .filter(Boolean);

        if (!selectedLines.length) {
            alert("Please select at least one line.");
            return;
        }

        const formData = new FormData();

        selectedLines.forEach(line => {
            formData.append("lines[]", line);
        });

        const csrfToken =
            document.querySelector(
                "[name=csrfmiddlewaretoken]"
            )?.value;

        const originalHTML = btn.innerHTML;

        btn.disabled = true;
        btn.innerHTML =
            '<i class="fa-solid fa-spinner fa-spin"></i> Exporting...';

        try {

            const response = await fetch(
                btn.dataset.url,
                {
                    method: "POST",
                    body: formData,
                    headers: {
                        "X-CSRFToken": csrfToken
                    }
                }
            );

            const data = await response.json();

            if (!data.ok) {
                throw new Error(
                    data.error || "Export failed."
                );
            }

            let msg =
                `Export complete\n\n` +
                `Saved:\n${data.export_dir}\n\n` +
                `Exported: ${data.exported_count}\n` +
                `Failed: ${data.failed_count}`;

            if (data.failed?.length) {

                msg += "\n\nFailed Lines:\n";

                data.failed.forEach(item => {
                    msg +=
                        `Line ${item.line}: ${item.error}\n`;
                });
            }

            alert(msg);

        }
        catch (err) {

            console.error(
                "Node position report export error:",
                err
            );

            alert(
                "Export failed:\n" + err.message
            );

        }
        finally {

            btn.disabled = false;
            btn.innerHTML = originalHTML;

        }

    });

    console.log(
        "[SWL] initExportNodePositionReports initialized"
    );
}