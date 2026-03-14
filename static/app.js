async function fetchJson(url){
  const res = await fetch(url);
  if(!res.ok) throw new Error('Network error');
  return res.json();
}

function el(tag, cls, txt){const e=document.createElement(tag);if(cls)e.className=cls;if(txt!==undefined)e.textContent=txt;return e}

function renderTable(container, data){
  if(!data || data.length===0){container.textContent='No data';return}
  const table=document.createElement('table');table.className='table';
  const thead=document.createElement('thead');
  const headerRow=document.createElement('tr');
  Object.keys(data[0]).forEach(k=>{const th=document.createElement('th');th.textContent=k;headerRow.appendChild(th)});
  thead.appendChild(headerRow);table.appendChild(thead);
  const tbody=document.createElement('tbody');
  data.forEach(row=>{const tr=document.createElement('tr');
    Object.keys(row).forEach(k=>{const td=document.createElement('td');td.textContent=row[k]===null?'':String(row[k]);tr.appendChild(td)});
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.innerHTML='';container.appendChild(table);
}

function renderBills(container, data){
  if(!data || data.length===0){container.textContent='No data';return}
  container.innerHTML=''; // Clear container
  data.forEach(bill=>{
    const card = document.createElement('div');
    card.className = 'bill-card';
    const header = document.createElement('h3');
    header.textContent = `${bill.bill_number}: ${bill.title}`;
    const date = document.createElement('p');
    date.innerHTML = `<strong>Last Action Date:</strong> ${bill.last_action_date || 'N/A'}`;
    const summary = document.createElement('p');
    summary.innerHTML = `<strong>AI Summary:</strong> ${bill.ai_summary || 'No summary available'}`;
    card.appendChild(header);
    card.appendChild(date);
    card.appendChild(document.createElement('br'));
    card.appendChild(summary);
    container.appendChild(card);
  });
}

function renderChampions(container, data){
  if(!data || data.length===0){container.textContent='No data';return}
  container.innerHTML=''; // Clear container
  data.forEach(champion=>{
    const card = document.createElement('div');
    card.className = 'champion-card';
    const left = document.createElement('div');
    left.className = 'champion-left';
    if(champion.image){
      const img = document.createElement('img');
      img.src = champion.image;
      img.alt = champion.sponsor_name;
      img.className = 'champion-img';
      img.onload = () => {
        if(img.naturalWidth > 0 && img.naturalHeight > 0){
          left.insertBefore(img, name);
        }
      };
      img.onerror = () => {
        left.classList.add('no-image');
      };
    } else {
      left.classList.add('no-image');
    }
    const name = document.createElement('div');
    name.className = 'champion-name';
    name.textContent = champion.sponsor_name;
    left.appendChild(name);
    const details = document.createElement('div');
    details.className = 'champion-details';
    details.innerHTML = `
      <p><strong>Party:</strong> ${champion.party || 'N/A'}</p>
      <p><strong>Climate Bills Passed:</strong> ${champion.climate_bills_passed}</p>
      <p><strong>District:</strong> ${champion.district || 'N/A'}</p>
      <p><strong>Chamber:</strong> ${champion.chamber || 'N/A'}</p>
      <p><strong>Gender:</strong> ${champion.gender || 'N/A'}</p>
      <p><strong>Email:</strong> ${champion.email ? `<a href="mailto:${champion.email}">${champion.email}</a>` : 'N/A'}</p>
      <p><strong>Birth Date:</strong> ${champion.birth_date || 'N/A'}</p>
      <p><strong>District Address:</strong> ${champion.district_address ? `<a href="https://www.google.com/maps?q=${encodeURIComponent(champion.district_address)}" target="_blank">${champion.district_address}</a>` : 'N/A'}</p>
      <p><strong>District Voice:</strong> ${champion.district_voice || 'N/A'}</p>
    `;
    card.appendChild(left);
    card.appendChild(details);
    container.appendChild(card);
  });
}

function renderInfluencers(container, data){
  if(!data || data.length===0){container.textContent='No data';return}
  container.innerHTML=''; // Clear container
  data.forEach(influencer=>{
    const card = document.createElement('div');
    card.className = 'champion-card';
    const left = document.createElement('div');
    left.className = 'champion-left';
    if(influencer.image){
      const img = document.createElement('img');
      img.src = influencer.image;
      img.alt = influencer.member_name;
      img.className = 'champion-img';
      img.onload = () => {
        if(img.naturalWidth > 0 && img.naturalHeight > 0){
          left.insertBefore(img, name);
        }
      };
      img.onerror = () => {
        left.classList.add('no-image');
      };
    } else {
      left.classList.add('no-image');
    }
    const name = document.createElement('div');
    name.className = 'champion-name';
    name.textContent = influencer.member_name;
    left.appendChild(name);
    const details = document.createElement('div');
    details.className = 'champion-details';
    let committeesHtml = '<p><strong>Climate Committees:</strong></p><ul>';
    if(influencer.climate_committees && influencer.climate_committees.length > 0){
      influencer.climate_committees.forEach(committee => {
        committeesHtml += `<li>${committee}</li>`;
      });
    } else {
      committeesHtml += '<li>No committees listed</li>';
    }
    committeesHtml += '</ul>';
    details.innerHTML = `
      <p><strong>Party:</strong> ${influencer.party || 'N/A'}</p>
      <p><strong>Climate Influence Score:</strong> ${influencer.climate_influence_score}</p>
      <p><strong>District:</strong> ${influencer.district || 'N/A'}</p>
      <p><strong>Chamber:</strong> ${influencer.chamber || 'N/A'}</p>
      <p><strong>Gender:</strong> ${influencer.gender || 'N/A'}</p>
      <p><strong>Email:</strong> ${influencer.email ? `<a href="mailto:${influencer.email}">${influencer.email}</a>` : 'N/A'}</p>
      <p><strong>Birth Date:</strong> ${influencer.birth_date || 'N/A'}</p>
      <p><strong>District Address:</strong> ${influencer.district_address ? `<a href="https://www.google.com/maps?q=${encodeURIComponent(influencer.district_address)}" target="_blank">${influencer.district_address}</a>` : 'N/A'}</p>
      <p><strong>District Voice:</strong> ${influencer.district_voice || 'N/A'}</p>
      ${committeesHtml}
    `;
    card.appendChild(left);
    card.appendChild(details);
    container.appendChild(card);
  });
}

async function loadChampions(){
  const container=document.getElementById('champions-container');
  container.textContent='Loading...';
  try{
    const data=await fetchJson('/api/climate_champions');
    renderChampions(container,data);
  }catch(e){container.textContent='Error: '+e.message}
}

async function loadBills(){
  const container=document.getElementById('bills-container');
  container.textContent='Loading...';
  try{
    const data=await fetchJson('/api/passed_climate_bills');
    renderBills(container,data);
  }catch(e){container.textContent='Error: '+e.message}
}

async function loadInfluencers(){
  const container=document.getElementById('influencers-container');
  container.textContent='Loading...';
  try{
    const data=await fetchJson('/api/climate_influencers');
    renderInfluencers(container,data);
  }catch(e){container.textContent='Error: '+e.message}
}

const refreshChampions = document.getElementById('refresh-champions');

if(refreshChampions) refreshChampions.addEventListener('click',loadChampions);

const refreshBills = document.getElementById('refresh-bills');

if(refreshBills) refreshBills.addEventListener('click',loadBills);
const refreshInfluencers = document.getElementById('refresh-influencers');

if(refreshInfluencers) refreshInfluencers.addEventListener('click', loadInfluencers);
window.addEventListener('load',()=>{loadChampions();}); // Load champions by default

// Tab switching

const tabChampions = document.getElementById('tab-champions');

if(tabChampions) {

  tabChampions.addEventListener('click', ()=>{

    document.getElementById('tab-champions').classList.add('active');

    document.getElementById('tab-bills').classList.remove('active');

    if(tabInfluencers) document.getElementById('tab-influencers').classList.remove('active');

    document.getElementById('content-champions').style.display = 'block';

    document.getElementById('content-bills').style.display = 'none';

    if(document.getElementById('content-influencers')) document.getElementById('content-influencers').style.display = 'none';

    if(document.getElementById('champions-container').textContent === 'Loading...') loadChampions();

  });

}

const tabBills = document.getElementById('tab-bills');

if(tabBills) {

  tabBills.addEventListener('click', ()=>{

    document.getElementById('tab-bills').classList.add('active');

    document.getElementById('tab-champions').classList.remove('active');

    if(tabInfluencers) document.getElementById('tab-influencers').classList.remove('active');

    document.getElementById('content-bills').style.display = 'block';

    document.getElementById('content-champions').style.display = 'none';

    if(document.getElementById('content-influencers')) document.getElementById('content-influencers').style.display = 'none';

    if(document.getElementById('bills-container').textContent === 'Loading...') loadBills();

  });

}

const tabInfluencers = document.getElementById('tab-influencers');

if(tabInfluencers) {

  tabInfluencers.addEventListener('click', ()=>{

    document.getElementById('tab-influencers').classList.add('active');

    document.getElementById('tab-champions').classList.remove('active');

    document.getElementById('tab-bills').classList.remove('active');

    document.getElementById('content-influencers').style.display = 'block';

    document.getElementById('content-champions').style.display = 'none';

    document.getElementById('content-bills').style.display = 'none';

    if(document.getElementById('influencers-container').textContent === 'Loading...') loadInfluencers();

  });

}