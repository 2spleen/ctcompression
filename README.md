# ctcompression
Code for an original compression algorithm code challenge 
(~20 hours spent optimizing a compression algorithm for a specific input file (financial bid/ask/trade tick data in CSV format roughly ordered by millisecond timestamps)

-- Results --  
Reduced an ~18MB sample file to ~9.8MB, more precisely:  
compression ratio: ~ 1.84  
space savings: => ~ 45.77%  

-- Possible Improvements --
- My simple/naive binary codeword generation — as I’m currently doing it, too quickly reaches codewords > 1 byte… 
- Performance of string operations on binary codewords converted to strings as I’m doing it currently is *horrible*, especially during decompress…
- My dictionary should ideally be placed in a header in the compressed file itself, not stored separately.
- Should re-format and expand code comments as docstrings and use pydoc module to generate class/method documentation.
- Should add (or have added earlier on) a couple of simple unit tests including a small built-in sample of data to operate on to verify changes/optimizations aren’t breaking things
