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
    const summary = document.createElement('p');
    summary.textContent = bill.ai_summary || 'No summary available';
    card.appendChild(header);
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
    const img = document.createElement('img');
    img.src = champion.image || 'https://via.placeholder.com/100x100?text=No+Image';
    img.alt = champion.sponsor_name;
    img.className = 'champion-img';
    const name = document.createElement('div');
    name.className = 'champion-name';
    name.textContent = champion.sponsor_name;
    left.appendChild(img);
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

document.getElementById('refresh-champions').addEventListener('click',loadChampions);
document.getElementById('refresh-bills').addEventListener('click',loadBills);
window.addEventListener('load',()=>{loadChampions();}); // Load champions by default

// Tab switching
document.getElementById('tab-champions').addEventListener('click', ()=>{
  document.getElementById('tab-champions').classList.add('active');
  document.getElementById('tab-bills').classList.remove('active');
  document.getElementById('content-champions').style.display = 'block';
  document.getElementById('content-bills').style.display = 'none';
  if(document.getElementById('champions-container').textContent === 'Loading...') loadChampions();
});

document.getElementById('tab-bills').addEventListener('click', ()=>{
  document.getElementById('tab-bills').classList.add('active');
  document.getElementById('tab-champions').classList.remove('active');
  document.getElementById('content-bills').style.display = 'block';
  document.getElementById('content-champions').style.display = 'none';
  if(document.getElementById('bills-container').textContent === 'Loading...') loadBills();
});