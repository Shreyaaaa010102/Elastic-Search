import React from 'react';
import {  ChevronLeft, ChevronRight } from 'lucide-react';

const API_BASE_URL = 'https://elastic-search-python-u30628.vm.elestio.app';

const ws = new WebSocket('wss://elastic-search-react-u30628.vm.elestio.app:3000/ws', {
  headers: {
    'Origin': 'https://elastic-search-react-u30628.vm.elestio.app'
  }
});

const SearchApp = () => {
  const [query, setQuery] = React.useState('');
  const [results, setResults] = React.useState([]);
  const [loading, setLoading] = React.useState(false);
  const [yearFrom, setYearFrom] = React.useState('');
  const [yearTo, setYearTo] = React.useState('');
  const [court, setCourt] = React.useState('');
  const [currentPage, setCurrentPage] = React.useState(1);
  const [totalResults, setTotalResults] = React.useState(0);
  const [hasSearched, setHasSearched] = React.useState(false);
  const [selectedSummary, setSelectedSummary] = React.useState(null);
  const [pdfUrl, setPdfUrl] = React.useState(null);
  const [sortOrder, setSortOrder] = React.useState('desc');
  const [suggestions, setSuggestions] = React.useState([]);

  const abortControllerRef = React.useRef(null);
  const searchTimeoutRef = React.useRef(null);
  const searchInputRef = React.useRef(null);
  
  const resultsPerPage = 10;
  const totalPages = Math.ceil(totalResults / resultsPerPage);

  const fetchResults = React.useCallback(async () => {
    try {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }

      abortControllerRef.current = new AbortController();
      
      setLoading(true);
      
      const params = new URLSearchParams({
        q: query,
        page: currentPage.toString(),
        size: resultsPerPage.toString(),
        sortOrder: sortOrder
      });

      if (yearFrom) params.append('yearFrom', yearFrom);
      if (yearTo) params.append('yearTo', yearTo);
      if (court) params.append('court', court);

      const response = await fetch(
        `${API_BASE_URL}/api/search?${params}`,
        { signal: abortControllerRef.current.signal }
      );
      
      const data = await response.json();

      if (!abortControllerRef.current.signal.aborted) {
        setResults(data.results || []);
        
        // Process facets to ensure SC/HC exist with zero counts


        setTotalResults(data.total || 0);
      }
    } catch (error) {
      if (error.name !== 'AbortError') {
        console.error('Search error:', error);
        setResults([]);
        setTotalResults(0);
      }
    } finally {
      if (!abortControllerRef.current?.signal.aborted) {
        setLoading(false);
      }
    }
  }, [query, currentPage, yearFrom, yearTo, court, sortOrder ]);

  React.useEffect(() => {
    if (hasSearched) {
      fetchResults();
    }
  }, [query, currentPage, yearFrom, yearTo, court, hasSearched, fetchResults, sortOrder]);

  const fetchSuggestions = React.useCallback(async (searchQuery) => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/autocomplete?q=${searchQuery}`);
      const data = await response.json();
      setSuggestions(data);
    } catch (error) {
      console.error('Autocomplete error:', error);
      setSuggestions([]);
    }
  }, []);
  
  const debouncedFetchSuggestions = React.useCallback((searchQuery) => {
    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current);
    }

    searchTimeoutRef.current = setTimeout(() => {
      if (searchQuery.length > 2) {
        fetchSuggestions(searchQuery);
      } else {
        setSuggestions([]);
      }
    }, 200);
  }, [fetchSuggestions]);

  const handleQueryChange = (e) => {
    const newQuery = e.target.value;
    setQuery(newQuery);
    
    if (!newQuery.trim()) {
      setResults([]);
      setTotalResults(0);
      setHasSearched(false);
    }
    
    debouncedFetchSuggestions(newQuery);
  };

  const handleSearch = (e) => {
    e.preventDefault();
    if (query.trim()) {
      setCurrentPage(1);
      setHasSearched(true);
    }
  };

  const handlePdfView = async (docId) => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/get-pdf-url?doc_id=${docId}`);
      const data = await response.json();
      setPdfUrl(data.url);
    } catch (error) {
      console.error('Failed to load PDF:', error);
      alert('PDF not found');
    }
  };

  const handleYearChange = (setter) => (e) => {
    setter(e.target.value);
    setCurrentPage(1);
  };

  const handleCourtChange = (e) => {
    setCourt(e.target.value);
    setCurrentPage(1);
  };

  const handlePageChange = (newPage) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage);
      window.scrollTo(0, 0);
    }
  };

  const getPageNumbers = () => {
    const delta = 2;
    const range = [];
    const rangeWithDots = [];
    let l;

    for (let i = 1; i <= totalPages; i++) {
      if (
        i === 1 ||
        i === totalPages ||
        (i >= currentPage - delta && i <= currentPage + delta)
      ) {
        range.push(i);
      }
    }

    for (let i of range) {
      if (l) {
        if (i - l === 2) {
          rangeWithDots.push(l + 1);
        } else if (i - l !== 1) {
          rangeWithDots.push('...');
        }
      }
      rangeWithDots.push(i);
      l = i;
    }

    return rangeWithDots;
  };

  React.useEffect(() => {
    return () => {
      if (searchTimeoutRef.current) {
        clearTimeout(searchTimeoutRef.current);
      }
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, []);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-6xl mx-auto p-4">
        <h1 className="text-2xl font-bold mb-6 text-gray-800">Legal Judgement Search Engine</h1>

        <form onSubmit={handleSearch} className="relative mb-6">
          <div className="flex gap-2">
            <input
              type="text"
              value={query}
              onChange={handleQueryChange}
              placeholder="Search judgements..."
              ref={searchInputRef}
              className="w-full p-3 border rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <button
              type="submit"
              className="px-6 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
            >
              Search
            </button>
          </div>
          {suggestions.length > 0 && (
            <div className="absolute top-full left-0 right-0 bg-white border rounded-lg shadow-lg mt-1 z-10">
              {suggestions.map((suggestion, index) => (
                <div
                  key={index}
                  className="p-2 hover:bg-gray-100 cursor-pointer"
                  onClick={() => {
                    setQuery(suggestion);
                    setSuggestions([]);
                  }}
                >
                  {suggestion}
                </div>
              ))}
            </div>
          )}
        </form>
        <div className="mb-6 bg-white p-4 rounded-lg shadow">
        <h2 className="font-semibold mb-4 text-gray-700">Filters</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {/* Year range inputs - keep existing */}
          <div className="space-y-2">
            <label className="block text-sm font-medium text-gray-600">Year Range</label>
            <div className="grid grid-cols-2 gap-2">
              <input
                type="number"
                placeholder="From"
                value={yearFrom}
                onChange={handleYearChange(setYearFrom)}
                min="1900"
                max={new Date().getFullYear()}
                className="w-full p-2 border rounded text-gray-700"
              />
              <input
                type="number"
                placeholder="To"
                value={yearTo}
                onChange={handleYearChange(setYearTo)}
                min="1900"
                max={new Date().getFullYear()}
                className="w-full p-2 border rounded text-gray-700"
              />
            </div>
          </div>

          {/* Court select - keep existing */}
          <div>
            <label className="block text-sm font-medium mb-2 text-gray-600">Court</label>
            <select
              value={court}
              onChange={handleCourtChange}
              className="w-full p-2 border rounded text-gray-700"
            >
              <option value="">All Courts</option>
              <option value="SC">Supreme Court</option>
              <option value="HC">High Court</option>
            </select>
          </div>

          {/* ▼▼▼ Add this new sort control ▼▼▼ */}
          <div>
            <label className="block text-sm font-medium mb-2 text-gray-600">Sort Year</label>
            <select
              value={sortOrder}
              onChange={(e) => {
                setSortOrder(e.target.value);
                setCurrentPage(1);
              }}
              className="w-full p-2 border rounded text-gray-700"
            >
              <option value="desc">Newest First</option>
              <option value="asc">Oldest First</option>
            </select>
          </div>
        </div>
      </div>
        {loading ? (
          <div className="text-center py-8">
            <div className="inline-block animate-spin rounded-full h-8 w-8 border-4 border-gray-300 border-t-blue-500"></div>
            <p className="mt-2 text-gray-600">Loading results...</p>
          </div>
        ) : !hasSearched ? (
          <div className="text-center py-8 text-gray-600">
            Enter a search term to find judgements
          </div>
        ) : results.length === 0 ? (
          <div className="text-center py-8 text-gray-600">No results found.</div>
        ) : (
          <div className="space-y-4">
            {results.map((result) => (
              <div key={result.id} className="bg-white p-4 rounded-lg shadow">
                <h3 className="text-lg font-semibold mb-2 text-gray-800">
                  {result.JudgmentSummary?.JudgmentName || "No title available"}
                </h3>
                <p className="text-gray-600 mb-2">
                  {result.JudgmentSummary?.Brief?.Introduction || "No summary available"}
                </p>
                {result.JudgmentMetadata?.Tags?.length > 0 && (
                  <div className="mt-2">
                    <h4 className="text-sm font-bold text-gray-600 mb-1">Keywords</h4>
                    <div className="flex flex-wrap gap-2">
                      {result.JudgmentMetadata.Tags.map((tag, idx) => (
                        <span
                          key={idx}
                          className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full text-sm"
                        >
                          {tag.Tag}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                {/* ▼▼▼ Move PDF Link OUTSIDE Tags Block ▼▼▼ */}
                {result.id && (
                  <button
                    onClick={() => handlePdfView(result.id)}
                    className="text-blue-600 hover:text-blue-800 text-sm mt-2 block"
                  >
                    View PDF 
                  </button>
                )}


                            
                {result.JudgmentSummary && (
                  <button
                    onClick={() => setSelectedSummary(result.JudgmentSummary)}
                    className="text-blue-600 hover:text-blue-800 text-sm mt-2 block"
                  >
                    View Detailed Summary 
                  </button>
                )}
              </div>
            ))}
          </div>
        )}

        {totalPages > 0 && (
          <div className="flex justify-center items-center mt-6 gap-2">
            <button
              onClick={() => handlePageChange(currentPage - 1)}
              disabled={currentPage === 1 || loading}
              className="p-2 rounded-lg border hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <ChevronLeft size={20} />
            </button>
            
            {getPageNumbers().map((pageNum, idx) => (
              <button
                key={idx}
                onClick={() => typeof pageNum === 'number' ? handlePageChange(pageNum) : null}
                disabled={loading}
                className={`px-3 py-1 rounded-lg ${
                  pageNum === currentPage
                    ? 'bg-blue-500 text-white'
                    : pageNum === '...'
                    ? 'cursor-default'
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                } disabled:opacity-50 disabled:cursor-not-allowed`}
              >
                {pageNum}
              </button>
            ))}

            <button
              onClick={() => handlePageChange(currentPage + 1)}
              disabled={currentPage === totalPages || loading}
              className="p-2 rounded-lg border hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <ChevronRight size={20} />
            </button>
          </div>
        )}
                
        {selectedSummary && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
            <div className="bg-white rounded-lg p-6 max-w-3xl w-full max-h-[90vh] overflow-y-auto">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-xl font-bold">Detailed Judgment Summary</h3>
                <button 
                  onClick={() => setSelectedSummary(null)}
                  className="text-gray-500 hover:text-gray-700 text-2xl"
                >
                  &times;
                </button>
              </div>
              
              <div className="space-y-4 text-gray-700">
                {/* Basic Information */}
                {selectedSummary.JudgmentName && (
                  <div>
                    <h4 className="font-semibold mb-1">Case Name</h4>
                    <p>{selectedSummary.JudgmentName}</p>
                  </div>
                )}


                {/* Brief Section */}
                {selectedSummary.Brief?.Introduction && (
                  <div>
                    <h4 className="font-semibold mb-1">Introduction</h4>
                    <p>{selectedSummary.Brief.Introduction}</p>
                  </div>
                )}

                {/* Background Section */}
                {selectedSummary.Background?.Context && (
                  <div>
                    <h4 className="font-semibold mb-1">Background Context</h4>
                    <p>{selectedSummary.Background.Context}</p>
                  </div>
                )}

                {selectedSummary.Background?.FactualMatrix && (
                  <div>
                    <h4 className="font-semibold mb-1">Factual Matrix</h4>
                    <p>{selectedSummary.Background.FactualMatrix}</p>
                  </div>
                )}

                {/* Key Issues */}
                {selectedSummary.KeyIssues?.length > 0 && (
                  <div>
                    <h4 className="font-semibold mb-1">Key Issues</h4>
                    <ul className="list-disc pl-6">
                      {selectedSummary.KeyIssues.map((issue, idx) => (
                        <li key={idx}>{issue.IssueDescription}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Legal Propositions */}
                {selectedSummary.LegalPropositions?.length > 0 && (
                  <div>
                    <h4 className="font-semibold mb-1">Legal Propositions</h4>
                    <ol className="list-decimal pl-6">
                      {selectedSummary.LegalPropositions.map((prop, idx) => (
                        <li key={idx} className="mb-2">
                          <p className="font-medium">{prop.PropositionDescription}</p>
                          {prop.Principle && <p className="text-sm text-gray-600">Principle: {prop.Principle}</p>}
                          {prop.Application && <p className="text-sm text-gray-600">Application: {prop.Application}</p>}
                        </li>
                      ))}
                    </ol>
                  </div>
                )}

                {/* Arguments Section */}
                {selectedSummary.Arguments && (
                  <div>
                    <h4 className="font-semibold mb-1">Arguments</h4>
                    <div className="space-y-4">
                      {selectedSummary.Arguments.Petitioner && (
                        <div>
                          <h5 className="font-medium mb-1">Petitioner's Arguments</h5>
                          <ul className="list-disc pl-6">
                            {selectedSummary.Arguments.Petitioner.MainArguments?.map((arg, idx) => (
                              <li key={idx}>{arg}</li>
                            ))}
                          </ul>
                        </div>
                      )}

                      {selectedSummary.Arguments.Respondent && (
                        <div>
                          <h5 className="font-medium mb-1">Respondent's Arguments</h5>
                          <ul className="list-disc pl-6">
                            {selectedSummary.Arguments.Respondent.MainArguments?.map((arg, idx) => (
                              <li key={idx}>{arg}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Judgment Details */}
                {selectedSummary.JudgmentDetails?.CourtRuling && (
                  <div>
                    <h4 className="font-semibold mb-1">Court Ruling</h4>
                    <p>{selectedSummary.JudgmentDetails.CourtRuling}</p>
                  </div>
                )}

                {selectedSummary.JudgmentDetails?.Reasoning && (
                  <div>
                    <h4 className="font-semibold mb-1">Reasoning</h4>
                    <p>{selectedSummary.JudgmentDetails.Reasoning}</p>
                  </div>
                )}

                {/* Conclusion Section */}
                {selectedSummary.Conclusion?.Summary && (
                  <div>
                    <h4 className="font-semibold mb-1">Conclusion</h4>
                    <p>{selectedSummary.Conclusion.Summary}</p>
                  </div>
                )}

                {selectedSummary.Conclusion?.KeyTakeaways && (
                  <div>
                    <h4 className="font-semibold mb-1">Key Takeaways</h4>
                    <ul className="list-disc pl-6">
                      {selectedSummary.Conclusion.KeyTakeaways.map((takeaway, idx) => (
                        <li key={idx}>{takeaway}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
        {pdfUrl && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
            <div className="bg-white rounded-lg p-6 max-w-3xl w-full max-h-[90vh] overflow-y-auto">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-xl font-bold">PDF Viewer</h3>
                <button 
                  onClick={() => setPdfUrl(null)}
                  className="text-gray-500 hover:text-gray-700 text-2xl"
                >
                  &times;
                </button>
              </div>
              <iframe 
                src={pdfUrl} 
                className="w-full h-[80vh] border-none"
                title="PDF document"
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default SearchApp;
