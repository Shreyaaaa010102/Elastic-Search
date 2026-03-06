import React from 'react';
import { Search } from 'lucide-react';

const SearchApp = () => {
  const [query, setQuery] = React.useState('');
  const [results, setResults] = React.useState([]);
  const [loading, setLoading] = React.useState(false);
  const [selectedYear, setSelectedYear] = React.useState('');
  const [selectedCourt, setSelectedCourt] = React.useState('');
  const [currentPage, setCurrentPage] = React.useState(1);
  const [totalResults, setTotalResults] = React.useState(0);
  const [facets, setFacets] = React.useState({ years: { buckets: [] }, courts: { buckets: [] } });

  React.useEffect(() => {
    // Fetch results and facets from the backend
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/search?q=${query}&page=${currentPage}&size=10${
            selectedYear ? `&year=${selectedYear}` : ''
          }${selectedCourt ? `&court=${selectedCourt}` : ''}`
        );
        const data = await response.json();
        setResults(data.results || []);
        setTotalResults(data.total || 0);
        setFacets(data.facets || { years: { buckets: [] }, courts: { buckets: [] } });
      } catch (error) {
        console.error('Error fetching data:', error);
        setResults([]);
        setTotalResults(0);
        setFacets({ years: { buckets: [] }, courts: { buckets: [] } });
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [query, selectedYear, selectedCourt, currentPage]);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-6xl mx-auto p-4">
        <h1 className="text-2xl font-bold mb-6 text-gray-800">Legal Judgement Search Engine</h1>

        {/* Search Bar */}
        <div className="relative mb-6">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search judgements..."
            className="w-full p-3 pr-12 border rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
          <Search className="absolute right-4 top-3 text-gray-400" size={24} />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          {/* Filters */}
          <div className="md:col-span-1">
            <div className="bg-white p-4 rounded-lg shadow">
              <h2 className="font-semibold mb-4 text-gray-700">Filters</h2>

              {/* Year Filter */}
              <div className="mb-4">
                <label className="block text-sm font-medium mb-2 text-gray-600">Year</label>
                <select
                  value={selectedYear}
                  onChange={(e) => setSelectedYear(e.target.value)}
                  className="w-full p-2 border rounded text-gray-700"
                >
                  <option value="">All Years</option>
                  {facets.years?.buckets?.map((year) => (
                    <option key={year.key} value={year.key}>
                      {year.key} ({year.doc_count})
                    </option>
                  ))}
                </select>
              </div>

              {/* Court Filter */}
              <div className="mb-4">
                <label className="block text-sm font-medium mb-2 text-gray-600">Court</label>
                <select
                  value={selectedCourt}
                  onChange={(e) => setSelectedCourt(e.target.value)}
                  className="w-full p-2 border rounded text-gray-700"
                >
                  <option value="">All Courts</option>
                  {facets.courts?.buckets?.map((court) => (
                    <option key={court.key} value={court.key}>
                      {court.key} ({court.doc_count})
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          {/* Results */}
          <div className="md:col-span-3">
            {loading ? (
              <div className="text-center py-8 text-gray-600">Loading...</div>
            ) : results.length === 0 ? (
              <div className="text-center py-8 text-gray-600">No results found.</div>
            ) : (
              <div className="space-y-4">
                {results.map((result) => (
                  <div key={result.id} className="bg-white p-4 rounded-lg shadow">
                    <h3 className="text-lg font-semibold mb-2 text-gray-800">
                      {result.judgement_name}
                    </h3>
                    <p className="text-gray-600 mb-2">{result.summary}</p>
                    <div className="flex flex-wrap gap-2">
                      {result.keywords.map((keyword, idx) => (
                        <span
                          key={idx}
                          className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full text-sm"
                        >
                          {keyword}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* Pagination */}
            <div className="flex justify-center mt-6 gap-2">
              {[...Array(Math.ceil(totalResults / 10)).keys()].map((page) => (
                <button
                  key={page + 1}
                  onClick={() => setCurrentPage(page + 1)}
                  className={`px-3 py-1 rounded ${
                    currentPage === page + 1
                      ? 'bg-blue-500 text-white'
                      : 'bg-gray-200 text-gray-700'
                  }`}
                >
                  {page + 1}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SearchApp;
