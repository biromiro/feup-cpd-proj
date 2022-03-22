#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <time.h>
#include <cstdlib>
#include <papi.h>
#include <math.h>

using namespace std;

#define SYSTEMTIME clock_t

 
void OnMult(int n) 
{
	
	SYSTEMTIME Time1, Time2;
	
	char st[100];
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

		
    pha = (double *)malloc((n * n) * sizeof(double));
	phb = (double *)malloc((n * n) * sizeof(double));
	phc = (double *)malloc((n * n) * sizeof(double));

	for(i=0; i<n; i++)
		for(j=0; j<n; j++)
			pha[i*n + j] = (double)1.0;



	for(i=0; i<n; i++)
		for(j=0; j<n; j++)
			phb[i*n + j] = (double)(i+1);



    Time1 = clock();

	for(i=0; i<n; i++)
	{	for( j=0; j<n; j++)
		{	temp = 0;
			for( k=0; k<n; k++)
			{	
				temp += pha[i*n+k] * phb[k*n+j];
			}
			phc[i*n+j]=temp;
		}
	}


    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC);
	cout << st;

	// display 10 elements of the result matrix to verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,n); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
	
	
}

// add code here for line x line matrix multiplication
void OnMultLine(int n)
{
	SYSTEMTIME Time1, Time2;
	
	char st[100];
	double temp;
	int i, j, k;

	double *pha, *phb, *phc;
	

		
    pha = (double *)malloc((n * n) * sizeof(double));
	phb = (double *)malloc((n * n) * sizeof(double));
	phc = (double *)malloc((n * n) * sizeof(double));

	for(i=0; i<n; i++)
		for(j=0; j<n; j++)
			pha[i*n + j] = (double)1.0;



	for(i=0; i<n; i++)
		for(j=0; j<n; j++)
			phb[i*n + j] = (double)(i+1);



    Time1 = clock();

	for(i=0; i<n; i++)
	{	for( k=0; k<n; k++)
		{	
			for( j=0; j<n; j++)
			{	
				phc[i*n+j] += pha[i*n+k] * phb[k*n+j];
			}
		}
	}


    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC);
	cout << st;

	// display 10 elements of the result matrix to verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,n); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
    
}

// add code here for block x block matrix multiplication
void OnMultBlock(int n, int block_size)
{
    
    	SYSTEMTIME Time1, Time2;
	
	char st[100];
	double temp;
	int i, j;

	double *pha, *phb, *phc;
	

		
    pha = (double *)malloc((n * n) * sizeof(double));
	phb = (double *)malloc((n * n) * sizeof(double));
	phc = (double *)malloc((n * n) * sizeof(double));

	for(i=0; i<n; i++)
		for(j=0; j<n; j++)
			pha[i*n + j] = (double)1.0;



	for(i=0; i<n; i++)
		for(j=0; j<n; j++)
			phb[i*n + j] = (double)(i+1);



    Time1 = clock();

	size_t a_line_block, k_block, b_col_block, a_line, k, b_col;
    size_t n_blocks = n / block_size;

	for(a_line_block = 0; a_line_block < n_blocks; a_line_block++)
	{	for( k_block=0; k_block < n_blocks; k_block++)
		{	
			for( b_col_block=0; b_col_block < n_blocks; b_col_block++)
			{
				size_t next_line_block = (a_line_block+1) * block_size;
				for (a_line = a_line_block*block_size; a_line < next_line_block; a_line++)
				{
					size_t k_next_block = (k_block + 1) * block_size;
					for ( k = k_block * block_size; k < k_next_block; k++)
					{
						size_t b_next_block = (b_col_block+1)*block_size;
						for (b_col = b_col_block * block_size; b_col < b_next_block; b_col++) 
						{
							phc[a_line * n + b_col] += pha[a_line * n + k] * phb[k * n + b_col];
						}
					}
				}
			}
		}
	}


    Time2 = clock();
	sprintf(st, "Time: %3.3f seconds\n", (double)(Time2 - Time1) / CLOCKS_PER_SEC);
	cout << st;

	// display 10 elements of the result matrix to verify correctness
	cout << "Result matrix: " << endl;
	for(i=0; i<1; i++)
	{	for(j=0; j<min(10,n); j++)
			cout << phc[j] << " ";
	}
	cout << endl;

    free(pha);
    free(phb);
    free(phc);
    
}



void handle_error (int retval)
{
  printf("PAPI error %d: %s\n", retval, PAPI_strerror(retval));
  exit(1);
}

void init_papi() {
  int retval = PAPI_library_init(PAPI_VER_CURRENT);
  if (retval != PAPI_VER_CURRENT && retval < 0) {
    printf("PAPI library version mismatch!\n");
    exit(1);
  }
  if (retval < 0) handle_error(retval);

  std::cout << "PAPI Version Number: MAJOR: " << PAPI_VERSION_MAJOR(retval)
            << " MINOR: " << PAPI_VERSION_MINOR(retval)
            << " REVISION: " << PAPI_VERSION_REVISION(retval) << "\n";
}


int main (int argc, char *argv[])
{

	char c;
	int n, blockSize;
	int op;
	
	int EventSet = PAPI_NULL;
  	long long values[2];
  	int ret;
	

	ret = PAPI_library_init( PAPI_VER_CURRENT );
	if ( ret != PAPI_VER_CURRENT )
		std::cout << "FAIL" << endl;


	ret = PAPI_create_eventset(&EventSet);
		if (ret != PAPI_OK) cout << "ERROR: create eventset" << endl;


	ret = PAPI_add_event(EventSet,PAPI_L1_DCM );
	if (ret != PAPI_OK) cout << "ERROR: PAPI_L1_DCM" << endl;


	ret = PAPI_add_event(EventSet,PAPI_L2_DCM);
	if (ret != PAPI_OK) cout << "ERROR: PAPI_L2_DCM" << endl;


	op=1;
	do {
		cout << endl << "1. Multiplication" << endl;
		cout << "2. Line Multiplication" << endl;
		cout << "3. Block Multiplication" << endl;
		cout << "Selection?: ";
		cin >>op;
		if (op == 0)
			break;
		printf("Dimensions: lins=cols ? ");
   		cin >> n;


		// Start counting
		ret = PAPI_start(EventSet);
		if (ret != PAPI_OK) cout << "ERROR: Start PAPI" << endl;

		switch (op){
			case 1: 
				OnMult(n);
				break;
			case 2:
				OnMultLine(n);  
				break;
			case 3:
				cout << "Block Size? ";
				cin >> blockSize;
				OnMultBlock(n, blockSize);  
				break;

		}

  		ret = PAPI_stop(EventSet, values);
  		if (ret != PAPI_OK) cout << "ERROR: Stop PAPI" << endl;
  		printf("L1 DCM: %lld \n",values[0]);
  		printf("L2 DCM: %lld \n",values[1]);

		ret = PAPI_reset( EventSet );
		if ( ret != PAPI_OK )
			std::cout << "FAIL reset" << endl; 



	}while (op != 0);

	ret = PAPI_remove_event( EventSet, PAPI_L1_DCM );
	if ( ret != PAPI_OK )
		std::cout << "FAIL remove event" << endl; 

	ret = PAPI_remove_event( EventSet, PAPI_L2_DCM );
	if ( ret != PAPI_OK )
		std::cout << "FAIL remove event" << endl; 

	ret = PAPI_destroy_eventset( &EventSet );
	if ( ret != PAPI_OK )
		std::cout << "FAIL destroy" << endl;

}