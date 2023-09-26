if __name__ == "__main__":
    print('hello world') 
    from src.blockchain.get_ethereum_data import Blockchain
    blockchain = Blockchain(abi='illuvium')
    x = blockchain._get_events(start_block=12084123)
    print(len(x))
