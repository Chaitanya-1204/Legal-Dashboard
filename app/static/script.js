document.addEventListener('DOMContentLoaded', function() {
    const cardGrid = document.getElementById('card-grid');
    const modal = document.getElementById('constructionModal');
    const closeModal = document.getElementById('closeModal');

    if (!cardGrid) {
        return;
    }

    const cardData = [
        {
            title: 'Acts',
            description: 'Find and read various central and state government acts.',
            logo: `<i class="fas fa-scroll fa-2x"></i>`,
            color: 'red',
            link: '/acts/'
        },
        {
            title: 'Supreme Court Judgments',
            description: 'Access the latest judgments from the Supreme Court of India.',
            logo: `<i class="fas fa-gavel fa-2x"></i>`,
            color: 'blue',
            link: '/sc/'
        },
        {
            title: 'Supreme Court Daily Orders',
            description: 'Stay updated with the latest daily orders from the SC.',
            logo: `<i class="fas fa-calendar-day fa-2x"></i>`,
            color: 'sky',
            link: '#'
        },
        {
            title: 'High Court Judgments',
            description: 'Browse through judgments from various High Courts.',
            logo: `<i class="fas fa-landmark fa-2x"></i>`,
            color: 'green',
            link: '/high_courts/'
        },
        {
            title: 'District Courts',
            description: 'Explore judgments from major district courts.',
            logo: `<i class="fas fa-building-columns fa-2x"></i>`,
            color: 'teal',
            link: '/districtcourt/'
        },
        {
            title: 'Tribunals',
            description: 'Access orders and information from various tribunals.',
            logo: `<i class="fas fa-users-cog fa-2x"></i>`,
            color: 'indigo',
            link: '/tribunals/'
        },
        {
            title: 'Blogs',
            description: 'Read insightful articles and blogs from legal experts.',
            logo: `<i class="fas fa-feather-alt fa-2x"></i>`,
            color: 'yellow',
            link: '#'
        },
        {
            title: 'Books',
            description: 'Explore a library of legal books and publications.',
            logo: `<i class="fas fa-book-open fa-2x"></i>`,
            color: 'purple',
            link: '#'
        }
    ];

    // Loop through the data to create and append each card
    cardData.forEach(item => {
        const cardElement = document.createElement('div');
        // Add the custom card class
        cardElement.className = `custom-card`;
        
        // Build the inner HTML of the card using the new custom classes
        cardElement.innerHTML = `
            <div class="custom-card-icon icon-bg-${item.color}">
                <span class="icon-text-${item.color}">${item.logo}</span>
            </div>
            <h3 class="custom-card-title">${item.title}</h3>
            <p class="custom-card-description">${item.description}</p>
        `;
        
        cardElement.addEventListener('click', () => {
            if (item.link && item.link !== '#') {
                window.location.href = item.link;
            } else {
                modal.style.display = 'flex';
            }
        });
        
        cardGrid.appendChild(cardElement);
    });

    // Modal functionality
    closeModal.addEventListener('click', () => {
        modal.style.display = 'none';
    });

    window.addEventListener('click', (event) => {
        if (event.target == modal) {
            modal.style.display = 'none';
        }
    });
});
