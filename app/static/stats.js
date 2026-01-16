// static/stats.js

document.addEventListener('DOMContentLoaded', () => {
  // ---------- THEME TOGGLE ----------
  const themeToggle = document.getElementById('theme-toggle');

  // Apply saved theme from localStorage
  const savedTheme = localStorage.getItem('stats-theme');
  if (savedTheme === 'dark') {
    document.body.classList.add('dark');
    if (themeToggle) themeToggle.textContent = '☀️ Light';
  } else {
    if (themeToggle) themeToggle.textContent = '🌙 Dark';
  }

  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const isDark = document.body.classList.toggle('dark');
      localStorage.setItem('stats-theme', isDark ? 'dark' : 'light');
      themeToggle.textContent = isDark ? '☀️ Light' : '🌙 Dark';
    });
  }

  // ---------- DATA DEFINITIONS ----------
  const legalData = {
    acts: { count: 18826, wordCount: 124830685 },
    supremeCourtJudgments: { count: 56584, wordCount: 198157112 },
    highCourtJudgments: { count: 11833266, wordCount: 10766801368 },
    tribunals: { count: 906167, wordCount: 968066520 },
    districtCourt: { count: 697024, wordCount: 598094939 },
    blogs: { count: 43434, wordCount: 24777851 },
    books: { count: 102, wordCount: 45000000 },
    others: { count: 2691855, wordCount: 2745423398 }
  };

  const actsCategoryData = [
    { category: 'Union of India - Act', count: 3932 }, { category: 'State of Rajasthan - Act', count: 1190 },
    { category: 'State of Tamilnadu- Act', count: 1078 }, { category: 'State of Punjab - Act', count: 986 },
    { category: 'State of Uttar Pradesh - Act', count: 944 }, { category: 'State of Madhya Pradesh - Act', count: 927 },
    { category: 'State of Odisha - Act', count: 861 }, { category: 'State of Bihar - Act', count: 766 },
    { category: 'State of Andhra Pradesh - Act', count: 757 }, { category: 'State of Haryana - Act', count: 745 },
    { category: 'State of Maharashtra - Act', count: 718 }, { category: 'State of West Bengal - Act', count: 630 },
    { category: 'State of Gujarat - Act', count: 523 }, { category: 'State of Jammu-Kashmir - Act', count: 443 },
    { category: 'State of Assam - Act', count: 409 }, { category: 'State of Karnataka - Act', count: 334 },
    { category: 'State of Jharkhand - Act', count: 327 }, { category: 'State of Goa - Act', count: 322 },
    { category: 'State of Telangana - Act', count: 320 }, { category: 'State of Chattisgarh - Act', count: 258 },
    { category: 'NCT Delhi - Act', count: 252 }, { category: 'State of Himachal Pradesh - Act', count: 242 },
    { category: 'State of Kerala - Act', count: 231 }, { category: 'Bombay Presidency - Act', count: 214 },
    { category: 'Bengal Presidency - Act', count: 150 }, { category: 'State of Uttarakhand - Act', count: 149 },
    { category: 'State of Meghalaya - Act', count: 133 }, { category: 'State of Sikkim - Act', count: 115 },
    { category: 'Constitution and Amendments', count: 107 }, { category: 'State of Mizoram - Act', count: 88 },
    { category: 'State of Tripura - Act', count: 83 }, { category: 'State of Manipur - Act', count: 83 },
    { category: 'State of Arunachal Pradesh - Act', count: 80 }, { category: 'International Treaty - Act', count: 78 },
    { category: 'State of Nagaland - Act', count: 54 }, { category: 'British India - Act', count: 49 },
    { category: 'UT Chandigarh - Act', count: 47 }, { category: 'State of Madhya Bharat - Act', count: 31 },
    { category: 'Daman and Diu - Act', count: 29 }, { category: 'State of Puducherry - Act', count: 27 },
    { category: 'Central Provinces And Berar - Act', count: 22 }, { category: 'Greater Bengaluru City Corporation - Act', count: 13 },
    { category: 'Andaman and Nicobar Islands - Act', count: 13 }, { category: 'Madras Presidency - Act', count: 12 },
    { category: 'Dadra And Nagar Haveli - Act', count: 12 }, { category: 'United Nations Conventions', count: 9 },
    { category: 'Chota Nagpur Division - Act', count: 8 }, { category: 'Lakshadweep - Act', count: 5 },
    { category: 'United Province - Act', count: 5 }, { category: 'UT Ladakh - Act', count: 5 },
    { category: 'Vindhya Province - Act', count: 4 }, { category: 'Nagpur Province - Act', count: 3 },
    { category: 'Mysore State - Act', count: 2 }, { category: 'Bhopal State - Act', count: 1 }
  ];

  const wordCountDistributionData = [
    { range: '<1k', num_files: 15673614 },
    { range: '1k-2k', num_files: 1698427 },
    { range: '2k-5k', num_files: 943794 },
    { range: '5k-10k', num_files: 265254 },
    { range: '10k-20k', num_files: 96273 },
    { range: '20k-50k', num_files: 36707 },
    { range: '>50k', num_files: 11015 }
  ];

  // ---------- HELPERS ----------
  const formatNumber = (num) => {
    if (num >= 1e12) return (num / 1e12).toFixed(2) + 'T';
    if (num >= 1e9) return (num / 1e9).toFixed(2) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
    return num.toString();
  };

  const formatNumberWithCommas = (num) => {
    if (num >= 1000) {
      return Math.round(num).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }
    return num.toString();
  };

  // ---------- AGGREGATES ----------
  const totalDocs = Object.values(legalData).reduce((s, item) => s + (item.count || 0), 0);
  const totalWords = Object.values(legalData).reduce((s, item) => s + (item.wordCount || 0), 0);

  // ---------- UPDATE TEXT STATS ----------
  const heroTotalDocsEl = document.getElementById('hero-total-docs');
  const heroTotalWordsEl = document.getElementById('hero-total-words');

  if (heroTotalDocsEl) heroTotalDocsEl.textContent = formatNumberWithCommas(totalDocs);
  if (heroTotalWordsEl) heroTotalWordsEl.textContent = formatNumberWithCommas(totalWords);

  const elTotalDocs = document.getElementById('total-docs');
  const elTotalWords = document.getElementById('total-words');
  const elSc = document.getElementById('sc-judgments');
  const elHc = document.getElementById('hc-judgments');
  const elTrib = document.getElementById('tribunal-cases');
  const elDist = document.getElementById('district-court-cases');
  const elActs = document.getElementById('acts-cases');
  const elBlogs = document.getElementById('blogs-count');
  const elBooks = document.getElementById('books-count');
  const elOthers = document.getElementById('others-count');

  if (elTotalDocs) elTotalDocs.textContent = formatNumber(totalDocs);
  if (elTotalWords) elTotalWords.textContent = formatNumber(totalWords);
  if (elSc) elSc.textContent = formatNumberWithCommas(legalData.supremeCourtJudgments.count);
  if (elHc) elHc.textContent = formatNumberWithCommas(legalData.highCourtJudgments.count);
  if (elTrib) elTrib.textContent = formatNumberWithCommas(legalData.tribunals.count);
  if (elDist) elDist.textContent = formatNumberWithCommas(legalData.districtCourt.count);
  if (elActs) elActs.textContent = formatNumberWithCommas(legalData.acts.count);
  if (elBlogs) elBlogs.textContent = formatNumberWithCommas(legalData.blogs.count);
  if (elBooks) elBooks.textContent = formatNumberWithCommas(legalData.books.count);
  if (elOthers) elOthers.textContent = formatNumberWithCommas(legalData.others.count);

  // ---------- KEY INSIGHT ----------
  let best = { key: null, avg: 0 };
  for (const [key, val] of Object.entries(legalData)) {
    const avg = val.wordCount / val.count;
    if (avg > best.avg) best = { key, avg };
  }
  const keyToLabel = {
    acts: 'Acts',
    supremeCourtJudgments: 'Supreme Court Judgments',
    highCourtJudgments: 'High Court Judgments',
    tribunals: 'Tribunal Cases',
    districtCourt: 'District Court Cases',
    blogs: 'Blogs',
    books: 'Books',
    others: 'Other Documents'
  };
  const insightEl = document.getElementById('insight-text');
  if (insightEl && best.key) {
    insightEl.textContent =
      `${keyToLabel[best.key]} have the highest average word count per document: ` +
      `${formatNumberWithCommas(best.avg.toFixed(0))} words on average.`;
  }

  // ---------- CHARTS COMMON ----------
  const chartLabels = [
    'Acts', 'Supreme Court', 'High Court',
    'Tribunals', 'District Court', 'Blogs', 'Books', 'Others'
  ];
  const chartDataList = Object.values(legalData);
  const chartColors = [
    '#3b82f6', '#ef4444', '#22c55e', '#f97316',
    '#a855f7', '#eab308', '#14b8a6', '#64748b'
  ];

  const barAnimation = {
    duration: 1200,
    easing: 'easeOutQuart',
    delay: (ctx) => {
      if (ctx.type === 'data' && ctx.mode === 'default') {
        return ctx.dataIndex * 70 + ctx.datasetIndex * 120;
      }
      return 0;
    }
  };

  // ---------- 1. Document Type Doughnut ----------
  const docTypeCanvas = document.getElementById('docTypeChart');
  let docTypeChart = null;

  if (docTypeCanvas) {
    const docTypeCtx = docTypeCanvas.getContext('2d');

    docTypeChart = new Chart(docTypeCtx, {
      type: 'doughnut',
      data: {
        labels: chartLabels,
        datasets: [{
          label: 'Document Count',
          data: chartDataList.map(d => d.count),
          backgroundColor: chartColors,
          borderColor: '#ffffff',
          borderWidth: 2,
          hoverOffset: 8
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: {
          animateRotate: true,
          animateScale: true,
          duration: 1200,
          easing: 'easeOutBack'
        },
        plugins: {
          legend: { position: 'bottom', labels: { padding: 15 } },
          tooltip: {
            callbacks: {
              label: (c) => `${c.label}: ${formatNumberWithCommas(c.parsed)}`
            }
          }
        }
      }
    });

    // Metric toggle buttons
    document.querySelectorAll('.doc-metric-toggle').forEach(btn => {
      btn.addEventListener('click', () => {
        const metric = btn.dataset.metric;
        document.querySelectorAll('.doc-metric-toggle').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');

        if (metric === 'count') {
          docTypeChart.data.datasets[0].data = chartDataList.map(d => d.count);
          docTypeChart.data.datasets[0].label = 'Document Count';
        } else {
          docTypeChart.data.datasets[0].data = chartDataList.map(d => d.wordCount);
          docTypeChart.data.datasets[0].label = 'Total Word Count';
        }
        docTypeChart.update();
      });
    });
  }

  // ---------- 2. Word Count Distribution (Histogram-like Bar) ----------
  const wordCountDistCanvas = document.getElementById('wordCountDistributionChart');
  if (wordCountDistCanvas) {
    const wordCountDistCtx = wordCountDistCanvas.getContext('2d');

    const wordCountDistLabels = wordCountDistributionData.map(d => d.range);
    const wordCountDistCounts = wordCountDistributionData.map(d => d.num_files);

    const attractiveColors = [
      'rgba(59, 130, 246, 0.8)', 'rgba(34, 197, 94, 0.8)', 'rgba(234, 179, 8, 0.8)',
      'rgba(249, 115, 22, 0.8)', 'rgba(239, 68, 68, 0.8)', 'rgba(168, 85, 247, 0.8)', 'rgba(20, 184, 166, 0.8)'
    ];
    const attractiveBorderColors = [
      'rgb(59, 130, 246)', 'rgb(34, 197, 94)', 'rgb(234, 179, 8)',
      'rgb(249, 115, 22)', 'rgb(239, 68, 68)', 'rgb(168, 85, 247)', 'rgb(20, 184, 166)'
    ];

    new Chart(wordCountDistCtx, {
      type: 'bar',
      data: {
        labels: wordCountDistLabels,
        datasets: [{
          label: 'Number of Files',
          data: wordCountDistCounts,
          backgroundColor: attractiveColors,
          borderColor: attractiveBorderColors,
          borderWidth: 2,
          borderRadius: 5,
          borderSkipped: false
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: barAnimation,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: ${formatNumberWithCommas(c.parsed.y)}`
            }
          }
        },
        scales: {
          y: {
            type: 'logarithmic',
            title: { display: true, text: 'Number of Files' },
            ticks: { display: false },
            grid: { display: false }
          },
          x: {
            title: { display: true, text: 'Word Count Range' },
            grid: { display: false }
          }
        }
      }
    });
  }

  // ---------- 3. Word Count by Document Type (Horizontal Bar) ----------
  const wordCountCanvas = document.getElementById('wordCountChart');
  if (wordCountCanvas) {
    const wordCountCtx = wordCountCanvas.getContext('2d');

    new Chart(wordCountCtx, {
      type: 'bar',
      data: {
        labels: chartLabels,
        datasets: [{
          label: 'Word Count',
          data: chartDataList.map(d => d.wordCount),
          backgroundColor: chartColors
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        animation: barAnimation,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: ${formatNumber(c.parsed.x)}`
            }
          }
        },
        scales: {
          x: {
            type: 'logarithmic',
            title: { display: true, text: 'Total Word Count (log scale)' },
            ticks: { display: false },
            grid: { display: false }
          }
        }
      }
    });
  }

  // ---------- 4. Average Word Count Per Document (Vertical Bar) ----------
  const avgWordCanvas = document.getElementById('avgWordCountChart');
  if (avgWordCanvas) {
    const avgWordCountCtx = avgWordCanvas.getContext('2d');

    const avgWordCountLabels = [];
    const avgWordCountData = [];

    chartLabels.forEach((label, index) => {
      if (label !== 'Books') {
        avgWordCountLabels.push(label);
        const d = chartDataList[index];
        avgWordCountData.push(d.wordCount / d.count);
      }
    });
    const avgWordCountColors = chartColors.filter((_, idx) => chartLabels[idx] !== 'Books');

    new Chart(avgWordCountCtx, {
      type: 'bar',
      data: {
        labels: avgWordCountLabels,
        datasets: [{
          label: 'Avg. Word Count',
          data: avgWordCountData,
          backgroundColor: avgWordCountColors
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: barAnimation,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: ${formatNumberWithCommas(c.parsed.y)}`
            }
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            title: { display: true, text: 'Average Words Per Document' }
          }
        }
      }
    });
  }

  // ---------- 5. Acts Distribution (Horizontal Bar) ----------
  const actsCanvas = document.getElementById('actsDistributionChart');
  if (actsCanvas) {
    const actsCtx = actsCanvas.getContext('2d');

    actsCategoryData.sort((a, b) => b.count - a.count);
    const topN = 15;
    const topActsData = actsCategoryData.slice(0, topN);
    const otherActsCount = actsCategoryData.slice(topN).reduce((s, item) => s + item.count, 0);

    if (otherActsCount > 0) {
      topActsData.push({ category: 'Other States/Presidencies', count: otherActsCount });
    }

    const actsLabels = topActsData.map(d => d.category.replace(/ - Act|- Act/g, ''));
    const actsCounts = topActsData.map(d => d.count);

    new Chart(actsCtx, {
      type: 'bar',
      data: {
        labels: actsLabels,
        datasets: [{
          label: 'Number of Acts',
          data: actsCounts,
          backgroundColor: '#8b5cf6',
          borderColor: '#7c3aed',
          borderWidth: 1
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        animation: barAnimation,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (c) => `${c.dataset.label}: ${formatNumberWithCommas(c.parsed.x)}`
            }
          }
        },
        scales: {
          x: { beginAtZero: true, title: { display: true, text: 'Number of Acts' } },
          y: { ticks: { font: { size: 10 } } }
        }
      }
    });
  }

  // ---------- CARD APPEAR ANIMATION ----------
  const cards = document.querySelectorAll('.card');
  cards.forEach((card, i) => {
    setTimeout(() => card.classList.add('card-visible'), i * 80);
  });
});
// ---------- AUTO-ROTATING CHARTS ----------
// ---------- AUTO-ROTATING CHARTS WITH CONTROLS ----------
// ---------- AUTO-ROTATING CHARTS WITH CONTROLS ----------
const slides = document.querySelectorAll('.chart-slide');
const dotsContainer = document.getElementById('carousel-dots');
const arrows = document.querySelectorAll('.carousel-arrow');

let currentSlide = 0;
let slideInterval = null;
let dots = [];
let isPaused = false;  // pause state controlled by clicking on slide

// Create dots dynamically
if (slides.length > 0 && dotsContainer) {
  slides.forEach((_, idx) => {
    const dot = document.createElement('button');
    dot.classList.add('carousel-dot');
    if (idx === 0) dot.classList.add('active');
    dot.dataset.index = idx.toString();
    dotsContainer.appendChild(dot);
    dots.push(dot);

    dot.addEventListener('click', () => {
      isPaused = true;
      stopCarousel();
      goToSlide(idx);
    });
  });
}

function updateDots(index) {
  dots.forEach((dot, i) => {
    dot.classList.toggle('active', i === index);
  });
}

function showSlide(index) {
  slides.forEach((slide, i) => {
    slide.classList.toggle('active', i === index);
  });
  updateDots(index);
}

function nextSlide() {
  currentSlide = (currentSlide + 1) % slides.length;
  showSlide(currentSlide);
}

function prevSlide() {
  currentSlide = (currentSlide - 1 + slides.length) % slides.length;
  showSlide(currentSlide);
}

function goToSlide(index) {
  currentSlide = index;
  showSlide(currentSlide);
}

function startCarousel() {
  if (slideInterval || slides.length === 0) return;
  slideInterval = setInterval(nextSlide, 2000); // 2 seconds
}

function stopCarousel() {
  if (slideInterval) {
    clearInterval(slideInterval);
    slideInterval = null;
  }
}

// Arrow controls
arrows.forEach(arrow => {
  arrow.addEventListener('click', () => {
    const dir = arrow.dataset.dir;
    isPaused = true;
    stopCarousel();
    if (dir === 'next') nextSlide();
    else prevSlide();
  });
});

// Click on slide -> toggle pause/resume
slides.forEach(slide => {
  slide.addEventListener('click', () => {
    isPaused = !isPaused;
    if (isPaused) {
      stopCarousel();
    } else {
      startCarousel();
    }
  });
});

// Init
if (slides.length > 0) {
  showSlide(0);
  startCarousel();  // auto-rotation starts immediately
}