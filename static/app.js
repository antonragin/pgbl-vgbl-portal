/* Minimal JS for allocation sliders and confirmations */

function updateAllocationTotal() {
    const sliders = document.querySelectorAll('.alloc-slider');
    let total = 0;
    sliders.forEach(s => {
        total += parseInt(s.value) || 0;
        const display = document.getElementById('val-' + s.dataset.fundId);
        if (display) display.textContent = s.value + '%';
    });
    const totalEl = document.getElementById('alloc-total');
    if (totalEl) {
        totalEl.textContent = total + '%';
        totalEl.style.color = total === 100 ? '#4CAF50' : '#f44336';
    }
    const submitBtn = document.getElementById('alloc-submit');
    if (submitBtn) submitBtn.disabled = total !== 100;
}

document.addEventListener('input', function(e) {
    if (e.target.classList.contains('alloc-slider')) {
        updateAllocationTotal();
    }
});

function confirmAction(msg) {
    return confirm(msg);
}


################################################################################
