function list = ml( array )
% The function converts an matlab array of numbers into a string of numbers
% This can be used in a select-clause using the IN() clause
%
% Input parameters are:
%   array:          The Matlab-Array
%
% Author: Stephan Jaeckel (stephan.jaeckel@hhi.fraunhofer.de)
% Latest Changes:
%   16.07.2009  Created
%   25.08.2009  Performance Update
%
array = reshape( array , 1 ,[]);                        % Reshape the array into a vector
list = sprintf('%1.16g,',array);
list = list(1:size(list,2)-1);                          % Remove last coma
